import { Redis as IORedis } from 'ioredis'
import { afterEach } from 'vitest'

import { redisCoordinator } from '../src/coordinators/redis.js'
import type { Coordinator } from '../src/types.js'
import { runCoordinatorContract } from './coordinator-contract.js'

const shouldRun = process.env.RUN_REDIS_INTEGRATION === '1'
const open: Array<{ coordinator: Coordinator; client: IORedis }> = []

runCoordinatorContract(
  'redis coordinator',
  () => {
    const client = new IORedis(
      process.env.REDIS_URL ?? 'redis://localhost:6379'
    )
    const coordinator = redisCoordinator(client)
    open.push({ coordinator, client })
    return coordinator
  },
  { run: shouldRun, notifications: true, closesOperations: true }
)

// Clean up even when an assertion failed mid-test.
afterEach(async () => {
  for (const { coordinator, client } of open.splice(0)) {
    await coordinator.close().catch(() => undefined)
    await client.quit().catch(() => undefined)
  }
})
