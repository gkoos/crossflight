import { Redis as IORedis } from 'ioredis'
import { createHash, randomUUID } from 'node:crypto'

import type {
  AcquireOptions,
  Coordinator,
  Lease,
  WaitOptions,
} from '../types.js'

export interface RedisCoordinatorOptions {
  namespace?: string
  hashKey?: (key: string) => string
  commandTimeoutMs?: number
}

export class RedisCommandTimeoutError extends Error {
  constructor(operation: string, timeoutMs: number) {
    super(`Redis command timed out: ${operation} exceeded ${timeoutMs}ms`)
    this.name = 'RedisCommandTimeoutError'
  }
}

function defaultHashKey(key: string): string {
  return createHash('sha256').update(key).digest('hex')
}

function isClosedConnectionMessage(message: string): boolean {
  const normalized = message.toLowerCase()

  if (normalized.includes('connection is closed') || normalized.includes('connection closed')) {
    return true
  }

  if (normalized.includes('disconnected')) {
    return true
  }

  return normalized.includes('connection') && normalized.includes('lost')
}

async function withCommandTimeout<T>(
  timeoutMs: number | undefined,
  operation: string,
  run: () => Promise<T>
): Promise<T> {
  if (!timeoutMs || timeoutMs <= 0) {
    return await run()
  }

  let timer: ReturnType<typeof setTimeout>
  const timeoutPromise = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      reject(new RedisCommandTimeoutError(operation, timeoutMs))
    }, timeoutMs)
  })

  try {
    return await Promise.race([run(), timeoutPromise])
  } finally {
    clearTimeout(timer)
  }
}

class RedisLease implements Lease {
  constructor(
    public readonly key: string,
    private readonly ownerToken: string,
    private readonly client: IORedis,
    private readonly ttlMs: number,
    private readonly changeChannel: string,
    private readonly commandTimeoutMs?: number
  ) {}

  async renew(): Promise<boolean> {
    const result = await withCommandTimeout(this.commandTimeoutMs, 'lease.renew.eval', async () => await this.client.eval(
      `
      if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('pexpire', KEYS[1], ARGV[2])
      end
      return 0
      `,
      1,
      this.key,
      this.ownerToken,
      String(this.ttlMs)
    ))

    if (Number(result) === 1) {
      await withCommandTimeout(
        this.commandTimeoutMs,
        'lease.renew.publish',
        async () => await this.client.publish(this.changeChannel, `${Date.now()}`)
      )
    }

    return Number(result) === 1
  }

  async complete(): Promise<void> {
    const result = await withCommandTimeout(this.commandTimeoutMs, 'lease.complete.eval', async () => await this.client.eval(
      `
      if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
      end
      return 0
      `,
      1,
      this.key,
      this.ownerToken
    ))

    if (Number(result) === 1) {
      await withCommandTimeout(
        this.commandTimeoutMs,
        'lease.complete.publish',
        async () => await this.client.publish(this.changeChannel, `${Date.now()}`)
      )
    }
  }

  async abandon(): Promise<void> {
    const result = await withCommandTimeout(this.commandTimeoutMs, 'lease.abandon.eval', async () => await this.client.eval(
      `
      if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
      end
      return 0
      `,
      1,
      this.key,
      this.ownerToken
    ))

    if (Number(result) === 1) {
      await withCommandTimeout(
        this.commandTimeoutMs,
        'lease.abandon.publish',
        async () => await this.client.publish(this.changeChannel, `${Date.now()}`)
      )
    }
  }
}

export class RedisCoordinator implements Coordinator {
  private readonly namespace: string
  private readonly hashKey: (key: string) => string
  private readonly commandTimeoutMs?: number
  private readonly subscribedChannels = new Set<string>()
  private readonly subscriptionClient: IORedis
  private closed = false
  private disconnected = false
  private readonly handleClientDisconnect = () => {
    this.disconnected = true
  }

  constructor(private readonly client: IORedis, options: RedisCoordinatorOptions = {}) {
    this.namespace = options.namespace ?? 'crossflight'
    this.hashKey = options.hashKey ?? defaultHashKey
    this.commandTimeoutMs = options.commandTimeoutMs
    this.subscriptionClient = new IORedis(client.options)

    this.client.on('error', this.handleClientDisconnect)
    this.client.on('end', this.handleClientDisconnect)
    this.subscriptionClient.on('error', this.handleClientDisconnect)
    this.subscriptionClient.on('end', this.handleClientDisconnect)
  }

  private resolveLeaseKey(key: string): string {
    return `${this.namespace}:flight:${this.hashKey(key)}`
  }

  private resolveChannel(key: string): string {
    return `${this.namespace}:change:${this.hashKey(key)}`
  }

  private assertOpen(): void {
    if (this.closed || this.disconnected) {
      throw new Error('Redis coordinator is closed')
    }

    if (this.client.status === 'close' || this.client.status === 'end') {
      this.disconnected = true
      throw new Error('Redis coordinator is closed')
    }
  }

  private handleClosedRedisError(error: unknown): never | void {
    const message = error instanceof Error ? error.message : String(error)
    if (isClosedConnectionMessage(message)) {
      this.disconnected = true
      throw new Error('Redis coordinator is closed')
    }
  }

  async acquire(key: string, options?: AcquireOptions): Promise<Lease | null> {
    this.assertOpen()

    if (options?.signal?.aborted) {
      throw new DOMException('The operation was aborted', 'AbortError')
    }

    const ttlMs = options?.ttlMs ?? 30_000
    const ownerToken = randomUUID()
    const baseKey = this.resolveLeaseKey(key)
    const changeChannel = this.resolveChannel(key)

    try {
      const result = await withCommandTimeout(this.commandTimeoutMs, 'acquire.eval', async () => await this.client.eval(
        `
        if redis.call('get', KEYS[1]) == false then
          return redis.call('set', KEYS[1], ARGV[1], 'PX', ARGV[2], 'NX') ~= false and 1 or 0
        end
        return 0
        `,
        1,
        baseKey,
        ownerToken,
        String(ttlMs)
      ))

      if (Number(result) !== 1) {
        return null
      }

      await withCommandTimeout(
        this.commandTimeoutMs,
        'acquire.publish',
        async () => await this.client.publish(changeChannel, `${Date.now()}`)
      )

      return new RedisLease(
        baseKey,
        ownerToken,
        this.client,
        ttlMs,
        changeChannel,
        this.commandTimeoutMs
      )
    } catch (error) {
      this.handleClosedRedisError(error)
      throw error
    }
  }

  async waitForChange(key: string, options?: WaitOptions): Promise<void> {
    this.assertOpen()

    const timeoutMs = options?.timeoutMs ?? 100
    const signal = options?.signal
    const channel = this.resolveChannel(key)

    if (signal?.aborted) {
      throw new DOMException('The operation was aborted', 'AbortError')
    }

    await new Promise<void>((resolve, reject) => {
      let settled = false

      const cleanup = () => {
        if (settled) {
          return
        }

        settled = true
        clearTimeout(timer)
        signal?.removeEventListener('abort', onAbort)
        this.subscriptionClient.off('message', onMessage)
        if (this.subscribedChannels.has(channel)) {
          this.subscribedChannels.delete(channel)
          void this.subscriptionClient.unsubscribe(channel).catch(() => undefined)
        }
      }

      const onAbort = () => {
        cleanup()
        reject(new DOMException('The operation was aborted', 'AbortError'))
      }

      const onMessage = (receivedChannel: string) => {
        if (receivedChannel !== channel) {
          return
        }

        cleanup()
        resolve()
      }

      const timer = setTimeout(() => {
        cleanup()
        resolve()
      }, timeoutMs)

      signal?.addEventListener('abort', onAbort, { once: true })
      this.subscriptionClient.on('message', onMessage)

      withCommandTimeout(this.commandTimeoutMs, 'waitForChange.subscribe', async () => await this.subscriptionClient.subscribe(channel))
        .then(() => {
          this.subscribedChannels.add(channel)
        })
        .catch((error) => {
          cleanup()
          this.handleClosedRedisError(error)
          reject(error)
        })
    })
  }

  async close(): Promise<void> {
    if (this.closed) {
      return
    }

    this.closed = true
    this.disconnected = true

    this.client.off('error', this.handleClientDisconnect)
    this.client.off('end', this.handleClientDisconnect)
    this.subscriptionClient.off('error', this.handleClientDisconnect)
    this.subscriptionClient.off('end', this.handleClientDisconnect)

    for (const channel of [...this.subscribedChannels]) {
      this.subscribedChannels.delete(channel)
      void this.subscriptionClient.unsubscribe(channel).catch(() => undefined)
    }

    try {
      if (this.subscriptionClient.status !== 'close' && this.subscriptionClient.status !== 'end') {
        await this.subscriptionClient.quit()
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      if (/Connection is closed|closed/i.test(message)) {
        return
      }

      throw error
    }
  }
}

export function redisCoordinator(
  client: IORedis,
  options: RedisCoordinatorOptions = {}
): Coordinator {
  return new RedisCoordinator(client, options)
}
