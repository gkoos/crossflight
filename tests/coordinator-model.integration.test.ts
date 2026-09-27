import { randomUUID } from 'node:crypto'

import { Redis as IORedis } from 'ioredis'
import { describe, expect, it } from 'vitest'

import { redisCoordinator } from '../src/coordinators/redis.js'
import type { Coordinator, Lease } from '../src/types.js'
import {
  EXPIRE_EVERYTHING_MS,
  runScenario,
  scenarioArbitrary,
  type Clock,
  type Operation,
  type Participant,
  type Scenario,
} from './support/coordinator-model.js'
import { createPropertySuite } from './support/seed.js'

/**
 * The same differential as the in-memory suite, against the coordinator that
 * really coordinates: the model is only worth anything if the implementation
 * users depend on is held to it. The scenarios carry waits as well as lease
 * mutations, so `waitForChange` is compared too - including the wake-up, which
 * a single instance cannot test at all.
 *
 * Time cannot be shared with Redis, which expires a lease against its own clock,
 * so the suite runs the same scenarios against the two clocks a Redis-backed
 * coordinator has to face:
 *
 * - one that can only expire everything at once, which deletes the namespace's
 *   lease keys: for every question the contract asks - is the key free, can the
 *   old owner still renew, does a new owner get a different token - deleting the
 *   lease is what waiting the ttl out does, and it keeps the suite free of real
 *   waiting. Every ttl is raised to `minTtlMs`, which a round trip cannot
 *   outlast, so no lease ever expires on its own here.
 * - one that really waits: the scenario's jumps are scaled to milliseconds Redis
 *   can be held to, slept out for real, and the model is advanced by the same
 *   jumps, so a lease expires in the implementation exactly when the model says
 *   it does. This is the half a delete cannot show.
 */
const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379'
const shouldRun = process.env.RUN_REDIS_INTEGRATION === '1'

/** The in-memory suite's cases are milliseconds; this one is bounded by sleeps. */
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

/**
 * The smallest ttl the real clock can hold precisely: a round trip outlasting a
 * lease would make the two runs disagree about a lease that expired on its own,
 * and a jump of half this leaves a margin for the drift real time adds.
 */
const REAL_EXPIRY_MIN_TTL_MS = 300

/**
 * The scenario's ttl values are ten to sixty milliseconds, which no round trip
 * leaves safely alone: scaling both the ttls and the jumps keeps the *shape* of
 * the scenario - which jump lands on which ttl - while moving it into a range
 * real time can be measured in.
 */
const REAL_EXPIRY_SCALE = 6

/**
 * The longest jump that is waited out instead of deleted: a jump only has to be
 * bounded so a scenario cannot spend a second per advance, and the jump every
 * scenario ends with - past every ttl - is never one to sleep through.
 */
const REAL_EXPIRY_SLEEP_LIMIT_MS = 600

const scaled = (scenario: Scenario): Scenario => ({
  operations: scenario.operations.map((operation) => {
    if (operation.kind === 'advance') {
      return operation.byMs >= EXPIRE_EVERYTHING_MS
        ? operation
        : { ...operation, byMs: operation.byMs * REAL_EXPIRY_SCALE }
    }

    // The acquisitions the scenario ends with ask for seconds, which is not a
    // ttl this clock has to be precise about.
    return operation.kind === 'acquire' && operation.ttlMs < 5_000
      ? { ...operation, ttlMs: operation.ttlMs * REAL_EXPIRY_SCALE }
      : operation
  }),
})

const redisRealExpiryParticipant = (
  client: IORedis,
  namespace: string
): Participant => ({
  name: `redis coordinator with real expiry (${namespace})`,
  key: (logicalKey) => `scenario:${logicalKey}`,
  create: () => redisCoordinator(client, { namespace }),
  minTtlMs: REAL_EXPIRY_MIN_TTL_MS,
  clock: {
    realise: (operations: Operation[]) =>
      operations.map((operation) =>
        operation.kind === 'advance' &&
        operation.byMs > REAL_EXPIRY_SLEEP_LIMIT_MS
          ? { kind: 'advance' as const, byMs: EXPIRE_EVERYTHING_MS }
          : operation
      ),
    async advance(byMs: number): Promise<void> {
      if (byMs < EXPIRE_EVERYTHING_MS) {
        await sleep(byMs)
        return
      }

      // Past every ttl: the wait would be real, and the delete does the same.
      const keys = await client.keys(`${namespace}:*`)

      if (keys.length > 0) {
        await client.del(...keys)
      }
    },
  } satisfies Clock,
})

/** What a scenario's owner does to its key, either way it can wake a waiter. */
const mutations: Array<{
  name: string
  key: string
  run: (lease: Lease) => Promise<unknown>
}> = [
  {
    name: 'completes the lease',
    key: 'cross:complete',
    run: (lease) => lease.complete(),
  },
  {
    name: 'abandons the lease',
    key: 'cross:abandon',
    run: (lease) => lease.abandon(),
  },
  {
    name: 'renews the lease',
    key: 'cross:renew',
    run: (lease) => lease.renew(),
  },
]

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
     * The same scenarios against Redis's own clock, at a scale it can be held to.
     * A lease that expires on its own is the one thing a delete cannot model, and
     * every renewal after it, every takeover of it and every wait that outlives it
     * is a rule the model states and this holds the implementation to.
     */
    itProperty(
      'agrees with the model when Redis expires the leases for real',
      scenarioArbitrary.map(scaled),
      async (scenario) => {
        const client = new IORedis(REDIS_URL)

        try {
          const run = await runScenario(
            scenario,
            redisRealExpiryParticipant(client, `cf-expiry-${randomUUID()}`)
          )

          expect(run.actual).toEqual(run.expected)
        } finally {
          await client.quit().catch(() => undefined)
        }
      },
      // Real waits and real sleeps cost real time, so depth is capped at twice
      // this count: ten times it would be a minute of sleeping rather than a
      // deeper look at expiry.
      { runs: 6, maxRuns: 12 }
    )

    /**
     * The wake-up is the other half of the contract, and the half that a single
     * instance cannot test: the waiter and the owner are separate coordinators
     * here, with separate connections, so the notification has to travel through
     * Redis to reach the waiter.
     */
    for (const mutation of mutations) {
      it(`wakes a waiter on another instance when its owner ${mutation.name}`, async () => {
        const namespace = `cf-wake-${randomUUID()}`
        const ownerClient = new IORedis(REDIS_URL)
        const waiterClient = new IORedis(REDIS_URL)
        const owner: Coordinator = redisCoordinator(ownerClient, { namespace })
        const waiter: Coordinator = redisCoordinator(waiterClient, {
          namespace,
        })

        try {
          const lease = await owner.acquire(mutation.key, { ttlMs: 5_000 })
          expect(lease).not.toBeNull()

          const waiting = waiter.waitForChange(mutation.key, {
            timeoutMs: 2_000,
          })
          // Let the subscription settle before the change is announced.
          await sleep(50)

          const startedAt = Date.now()
          await mutation.run(lease!)

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
  }
)
