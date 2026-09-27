import { Redis as IORedis } from 'ioredis'
import { afterEach } from 'vitest'

import { redisCoordinator } from '../src/coordinators/redis.js'
import { runCoordinatorContract } from './coordinator-contract.js'

const shouldRun = process.env.RUN_REDIS_INTEGRATION === '1'
const clients: IORedis[] = []

runCoordinatorContract(
  'redis coordinator',
  () => {
    const client = new IORedis(
      process.env.REDIS_URL ?? 'redis://localhost:6379'
    )
    clients.push(client)
    return redisCoordinator(client)
  },
  { run: shouldRun, notifications: true, closesOperations: true }
)

afterEach(async () => {
  await Promise.all(
    clients.splice(0).map((client) => client.quit().catch(() => undefined))
  )
})
