import { performance } from 'node:perf_hooks'
import { Worker } from 'node:worker_threads'

import { Redis as IORedis } from 'ioredis'

import { createCrossflight } from '../src/core.js'
import { InMemoryCoordinator } from '../src/coordinators/in-memory.js'
import { redisCoordinator } from '../src/coordinators/redis.js'

export interface BenchmarkOptions {
  key?: string
  iterations?: number
  instances?: number
  requestsPerInstance?: number
  concurrency?: number
  loaderMs?: number
  redisUrl?: string
  resourceMetrics?: boolean
}

async function runExpensiveCryptoWork(targetMs: number): Promise<string> {
  return await new Promise((resolve, reject) => {
    const worker = new Worker(new URL('./crypto-worker.mjs', import.meta.url))

    worker.once('message', message => {
      resolve(message.result)
      worker.terminate().catch(() => undefined)
    })

    worker.once('error', error => {
      reject(error)
      worker.terminate().catch(() => undefined)
    })

    worker.once('exit', code => {
      if (code !== 0) {
        reject(new Error(`Worker exited unexpectedly with code ${code}`))
      }
    })

    worker.postMessage({ targetMs })
  })
}

async function ensureRedisAvailable(redisUrl: string): Promise<IORedis> {
  const client = new IORedis(redisUrl, { lazyConnect: true })

  try {
    await client.connect()
    await client.ping()
    return client
  } catch (error) {
    client.disconnect()
    throw new Error(
      `Redis benchmark requires a running Redis instance at ${redisUrl}. Start it with docker compose up -d redis or set REDIS_URL.`,
      { cause: error }
    )
  }
}

function createRedisCache(client: IORedis) {
  return {
    async get<T>(key: string) {
      const value = await client.get(key)
      if (value === null) {
        return { hit: false as const }
      }

      return { hit: true as const, value: JSON.parse(value) as T }
    },
    async set<T>(key: string, value: T, options?: { ttl?: number }) {
      const payload = JSON.stringify(value)
      if (options?.ttl) {
        await client.set(key, payload, 'PX', options.ttl)
        return
      }

      await client.set(key, payload)
    },
  }
}

async function mapWithConcurrency<T, TResult>(
  items: T[],
  workerCount: number,
  fn: (item: T) => Promise<TResult>
): Promise<TResult[]> {
  const results = new Array<TResult>(items.length)
  let nextIndex = 0

  const workers = Array.from({ length: Math.max(1, Math.min(workerCount, items.length)) }, async () => {
    while (true) {
      const currentIndex = nextIndex
      nextIndex += 1

      if (currentIndex >= items.length) {
        return
      }

      results[currentIndex] = await fn(items[currentIndex])
    }
  })

  await Promise.all(workers)
  return results
}

async function runUncoalescedScenario(
  key: string,
  instances: number,
  requestsPerInstance: number,
  loaderMs: number,
  redisUrl: string,
  concurrency: number
): Promise<number> {
  const counts = await Promise.all(
    Array.from({ length: instances }, async () => {
      const client = await ensureRedisAvailable(redisUrl)
      const cache = createRedisCache(client)

      try {
        await client.del(key)

        let loaderRuns = 0

        const tasks = await mapWithConcurrency(
          Array.from({ length: requestsPerInstance }, (_, index) => index),
          concurrency,
          async () => {
            const cached = await cache.get<string>(key)
            if (cached.hit) {
              return
            }

            loaderRuns += 1
            await runExpensiveCryptoWork(loaderMs)
            await cache.set(key, key, { ttl: 30_000 })
          }
        )

        void tasks

        return loaderRuns
      } finally {
        client.disconnect()
      }
    })
  )

  return counts.reduce((sum, count) => sum + count, 0)
}

async function runLocalCoalescedScenario(
  key: string,
  instances: number,
  requestsPerInstance: number,
  loaderMs: number,
  redisUrl: string,
  concurrency: number
): Promise<number> {
  const counts = await Promise.all(
    Array.from({ length: instances }, async () => {
      const client = await ensureRedisAvailable(redisUrl)
      const cache = createRedisCache(client)
      const crossflight = createCrossflight({ cache, coordinator: new InMemoryCoordinator() })
      let loaderRuns = 0

      try {
        await client.del(key)

        await mapWithConcurrency(
          Array.from({ length: requestsPerInstance }, (_, index) => index),
          concurrency,
          async () => {
            await crossflight.wrap(key, async () => {
              loaderRuns += 1
              await runExpensiveCryptoWork(loaderMs)
              await cache.set(key, key, { ttl: 30_000 })
              return key
            })
          }
        )
      } finally {
        await crossflight.close()
        client.disconnect()
      }

      return loaderRuns
    })
  )

  return counts.reduce((sum, count) => sum + count, 0)
}

async function runDistributedCoalescedScenario(
  key: string,
  instances: number,
  requestsPerInstance: number,
  loaderMs: number,
  redisUrl: string,
  concurrency: number
): Promise<number> {
  const counts = await Promise.all(
    Array.from({ length: instances }, async () => {
      const client = await ensureRedisAvailable(redisUrl)
      const cache = createRedisCache(client)
      const crossflight = createCrossflight({
        cache,
        coordinator: redisCoordinator(client),
      })
      let loaderRuns = 0

      try {
        await client.del(key)

        await mapWithConcurrency(
          Array.from({ length: requestsPerInstance }, (_, index) => index),
          concurrency,
          async () => {
            await crossflight.wrap(key, async () => {
              loaderRuns += 1
              await runExpensiveCryptoWork(loaderMs)
              await cache.set(key, key, { ttl: 30_000 })
              return key
            })
          }
        )
      } finally {
        await crossflight.close()
        client.disconnect()
      }

      return loaderRuns
    })
  )

  return counts.reduce((sum, count) => sum + count, 0)
}

async function measureScenario(
  label: string,
  fn: () => Promise<number>,
  samples = 5,
  includeResourceMetrics = false
): Promise<{
  fullBatchElapsedMs: number
  expensiveLoadExecutions: number
  userCpuMs?: number
}> {
  const elapsed: number[] = []
  const executions: number[] = []
  const userCpu: number[] = []

  for (let index = 0; index < samples; index += 1) {
    console.log(`Running ${label} sample ${index + 1}/${samples}...`)
    const beforeResourceUsage = process.resourceUsage()
    const started = performance.now()
    const expensiveLoadExecutions = await fn()
    const sampleElapsedMs = performance.now() - started
    const afterResourceUsage = process.resourceUsage()

    elapsed.push(sampleElapsedMs)
    executions.push(expensiveLoadExecutions)

    if (includeResourceMetrics) {
      userCpu.push((afterResourceUsage.userCPUTime - beforeResourceUsage.userCPUTime) / 1_000)
    }

    console.log(`${label} sample ${index + 1}/${samples} completed in ${sampleElapsedMs.toFixed(0)}ms`)
  }

  return {
    fullBatchElapsedMs: elapsed.reduce((sum, value) => sum + value, 0) / elapsed.length,
    expensiveLoadExecutions: executions.reduce((sum, value) => sum + value, 0) / executions.length,
    userCpuMs: includeResourceMetrics ? userCpu.reduce((sum, value) => sum + value, 0) / userCpu.length : undefined,
  }
}

export async function runBenchmark(options: BenchmarkOptions = {}): Promise<void> {
  const key = options.key ?? 'bench:coalesce'
  const samples = options.iterations ?? 1
  const instances = Math.max(1, options.instances ?? 5)
  const requestsPerInstance = Math.max(1, options.requestsPerInstance ?? 5)
  const loaderMs = options.loaderMs ?? 1_000
  const concurrency = Math.max(1, options.concurrency ?? 8)
  const redisUrl = options.redisUrl ?? process.env.REDIS_URL ?? 'redis://127.0.0.1:6379'
  const includeResourceMetrics = options.resourceMetrics ?? true

  const noCoalescing = await measureScenario(
    'no-coalescing',
    () => runUncoalescedScenario(key, instances, requestsPerInstance, loaderMs, redisUrl, concurrency),
    samples,
    includeResourceMetrics
  )

  const localCoalescing = await measureScenario(
    'local-coalescing',
    () => runLocalCoalescedScenario(key, instances, requestsPerInstance, loaderMs, redisUrl, concurrency),
    samples,
    includeResourceMetrics
  )

  const distributedCoalescing = await measureScenario(
    'distributed-coalescing (redis)',
    () => runDistributedCoalescedScenario(key, instances, requestsPerInstance, loaderMs, redisUrl, concurrency),
    samples,
    includeResourceMetrics
  )

  const results = [
    {
      name: 'no-coalescing',
      expensiveLoadExecutions: Number(noCoalescing.expensiveLoadExecutions.toFixed(2)),
      fullBatchElapsedMs: Number(noCoalescing.fullBatchElapsedMs.toFixed(2)),
      ...(includeResourceMetrics ? { userCpuMs: Number((noCoalescing.userCpuMs ?? 0).toFixed(2)) } : {}),
    },
    {
      name: 'local-coalescing',
      expensiveLoadExecutions: Number(localCoalescing.expensiveLoadExecutions.toFixed(2)),
      fullBatchElapsedMs: Number(localCoalescing.fullBatchElapsedMs.toFixed(2)),
      ...(includeResourceMetrics ? { userCpuMs: Number((localCoalescing.userCpuMs ?? 0).toFixed(2)) } : {}),
    },
    {
      name: 'distributed-coalescing (redis)',
      expensiveLoadExecutions: Number(distributedCoalescing.expensiveLoadExecutions.toFixed(2)),
      fullBatchElapsedMs: Number(distributedCoalescing.fullBatchElapsedMs.toFixed(2)),
      ...(includeResourceMetrics ? { userCpuMs: Number((distributedCoalescing.userCpuMs ?? 0).toFixed(2)) } : {}),
    },
  ]

  console.log(`Coalescing benchmark: ${key}`)
  console.log(`Benchmark shape: instances=${instances}, requestsPerInstance=${requestsPerInstance}, concurrency=${concurrency}`)
  console.table(results)
}
