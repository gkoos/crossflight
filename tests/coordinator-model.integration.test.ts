import { randomUUID } from 'node:crypto'

import { Redis as IORedis } from 'ioredis'
import { describe, expect, it } from 'vitest'

import { redisCoordinator } from '../src/coordinators/redis.js'
import type { Coordinator } from '../src/types.js'
import {
  EXPIRE_EVERYTHING_MS,
  runScenario,
  scenarioArbitrary,
  type Clock,
  type Operation,
  type Participant,
} from './support/coordinator-model.js'
import { createPropertySuite } from './support/seed.js'

/**
 * The same differential as the in-memory suite, against the coordinator that
 * really coordinates: the model is only worth anything if the implementation
 * users depend on is held to it.
 *
 * Time is the one thing that cannot be shared with Redis, which expires a lease
 * against its own clock. A scenario's jump is therefore realised by deleting the
 * namespace's lease keys: for every question the contract asks - is the key free,
 * can the old owner still renew, does a new owner get a different token -
 * deleting the lease is what waiting the ttl out does, and it keeps the suite
 * free of real waiting. Leases never expire on their own here either, because
 * every ttl is raised to `minTtlMs`, which a round trip cannot outlast.
 */
const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379'
const shouldRun = process.env.RUN_REDIS_INTEGRATION === '1'

const itProperty = createPropertySuite('coordinator-model-redis', { runs: 12 })

const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms))

const redisParticipant = (client: IORedis, namespace: string): Participant => ({
  name: `redis coordinator (${namespace})`,
  key: (logicalKey) => `scenario:${logicalKey}`,
  create: () => redisCoordinator(client, { namespace }),
  minTtlMs: 5_000,
  clock: {
    realise: (operations: Operation[]) =>
      operations.map((operation) =>
        operation.kind === 'advance'
          ? { kind: 'advance' as const, byMs: EXPIRE_EVERYTHING_MS }
          : operation
      ),
    async advance(): Promise<void> {
      const keys = await client.keys(`${namespace}:*`)

      if (keys.length > 0) {
        await client.del(...keys)
      }
    },
  } satisfies Clock,
})

describe.runIf(shouldRun)(
  'the redis coordinator against the contract model',
  () => {
    itProperty(
      'agrees with the model over generated lease sequences',
      scenarioArbitrary,
      async (scenario) => {
        const client = new IORedis(REDIS_URL)

        try {
          const run = await runScenario(
            scenario,
            redisParticipant(client, `cf-model-${randomUUID()}`)
          )

          expect(run.actual).toEqual(run.expected)
        } finally {
          await client.quit().catch(() => undefined)
        }
      }
    )

    /**
     * The wake-up is the other half of the contract, and the half that a single
     * instance cannot test: the waiter and the owner are separate coordinators
     * here, with separate connections, so the notification has to travel through
     * Redis to reach the waiter.
     */
    it('wakes a waiter on another instance when its owner completes the lease', async () => {
      const namespace = `cf-wake-${randomUUID()}`
      const ownerClient = new IORedis(REDIS_URL)
      const waiterClient = new IORedis(REDIS_URL)
      const owner: Coordinator = redisCoordinator(ownerClient, { namespace })
      const waiter: Coordinator = redisCoordinator(waiterClient, { namespace })

      try {
        const lease = await owner.acquire('cross:complete', { ttlMs: 5_000 })
        expect(lease).not.toBeNull()

        const waiting = waiter.waitForChange('cross:complete', {
          timeoutMs: 2_000,
        })
        // Let the subscription settle before the change is announced.
        await sleep(50)

        const startedAt = Date.now()
        await lease!.complete()

        await expect(waiting).resolves.toBeUndefined()
        expect(Date.now() - startedAt).toBeLessThan(1_000)
      } finally {
        await owner.close().catch(() => undefined)
        await waiter.close().catch(() => undefined)
        await ownerClient.quit().catch(() => undefined)
        await waiterClient.quit().catch(() => undefined)
      }
    })

    it('wakes a waiter on another instance when its owner abandons the lease', async () => {
      const namespace = `cf-wake-${randomUUID()}`
      const ownerClient = new IORedis(REDIS_URL)
      const waiterClient = new IORedis(REDIS_URL)
      const owner: Coordinator = redisCoordinator(ownerClient, { namespace })
      const waiter: Coordinator = redisCoordinator(waiterClient, { namespace })

      try {
        const lease = await owner.acquire('cross:abandon', { ttlMs: 5_000 })
        expect(lease).not.toBeNull()

        const waiting = waiter.waitForChange('cross:abandon', {
          timeoutMs: 2_000,
        })
        await sleep(50)

        const startedAt = Date.now()
        await lease!.abandon()

        await expect(waiting).resolves.toBeUndefined()
        expect(Date.now() - startedAt).toBeLessThan(1_000)
      } finally {
        await owner.close().catch(() => undefined)
        await waiter.close().catch(() => undefined)
        await ownerClient.quit().catch(() => undefined)
        await waiterClient.quit().catch(() => undefined)
      }
    })
  }
)
