import { Cluster as IORedisCluster, Redis as IORedis } from 'ioredis'
import { createHash, randomUUID } from 'node:crypto'

import type {
  AcquireOptions,
  Coordinator,
  Lease,
  WaitOptions,
} from '../types.js'

/**
 * The clients the coordinator runs on. `Redis` and `Cluster` both satisfy
 * it - the coordinator only issues single-key commands and pub/sub - so a
 * cluster client is a drop-in replacement for a single-node one.
 */
export type RedisClient = IORedis | IORedisCluster

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

/**
 * A cluster client is recognised by `nodes()`, which only `Cluster` has: it
 * decides how the separate pub/sub connection has to be built.
 */
function isClusterClient(client: RedisClient): client is IORedisCluster {
  return typeof (client as IORedisCluster).nodes === 'function'
}

/**
 * A subscribed connection cannot run ordinary commands, so the coordinator
 * always owns a second connection. A single-node client is rebuilt from its
 * own options, a cluster client is duplicated so the new connection starts
 * from the same nodes and options.
 */
function createSubscriberClient(client: RedisClient): RedisClient {
  if (isClusterClient(client)) {
    return client.duplicate()
  }

  return new IORedis(client.options)
}

function isClosedConnectionMessage(message: string): boolean {
  const normalized = message.toLowerCase()

  if (
    normalized.includes('connection is closed') ||
    normalized.includes('connection closed')
  ) {
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

  return await new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new RedisCommandTimeoutError(operation, timeoutMs))
    }, timeoutMs)

    run().then(
      (value) => {
        clearTimeout(timer)
        resolve(value)
      },
      (error) => {
        clearTimeout(timer)
        reject(error)
      }
    )
  })
}

class RedisLease implements Lease {
  constructor(
    public readonly key: string,
    private readonly ownerToken: string,
    private readonly client: RedisClient,
    private readonly ttlMs: number,
    private readonly changeChannel: string,
    private readonly commandTimeoutMs?: number
  ) {}

  async renew(): Promise<boolean> {
    const result = await withCommandTimeout(
      this.commandTimeoutMs,
      'lease.renew.eval',
      async () =>
        await this.client.eval(
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
        )
    )

    if (Number(result) === 1) {
      await withCommandTimeout(
        this.commandTimeoutMs,
        'lease.renew.publish',
        async () =>
          await this.client.publish(this.changeChannel, `${Date.now()}`)
      )
    }

    return Number(result) === 1
  }

  async complete(): Promise<void> {
    const result = await withCommandTimeout(
      this.commandTimeoutMs,
      'lease.complete.eval',
      async () =>
        await this.client.eval(
          `
      if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
      end
      return 0
      `,
          1,
          this.key,
          this.ownerToken
        )
    )

    if (Number(result) === 1) {
      await withCommandTimeout(
        this.commandTimeoutMs,
        'lease.complete.publish',
        async () =>
          await this.client.publish(this.changeChannel, `${Date.now()}`)
      )
    }
  }

  async abandon(): Promise<void> {
    const result = await withCommandTimeout(
      this.commandTimeoutMs,
      'lease.abandon.eval',
      async () =>
        await this.client.eval(
          `
      if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
      end
      return 0
      `,
          1,
          this.key,
          this.ownerToken
        )
    )

    if (Number(result) === 1) {
      await withCommandTimeout(
        this.commandTimeoutMs,
        'lease.abandon.publish',
        async () =>
          await this.client.publish(this.changeChannel, `${Date.now()}`)
      )
    }
  }
}

export class RedisCoordinator implements Coordinator {
  private readonly namespace: string
  private readonly hashKey: (key: string) => string
  private readonly commandTimeoutMs?: number
  private readonly subscribedChannels = new Set<string>()
  private readonly channelWaiters = new Map<string, number>()
  private readonly pendingWaits = new Set<() => void>()
  private readonly subscriptionClient: RedisClient
  private closed = false
  private commandEnded = false
  private commandReconnecting = false
  private subscriptionEnded = false
  private subscriptionReconnecting = false
  private readonly handleCommandError = () => {
    this.commandReconnecting = true
  }
  private readonly handleCommandReady = () => {
    this.commandReconnecting = false
  }
  private readonly handleCommandEnd = () => {
    this.commandEnded = true
  }
  private readonly handleSubscriptionError = () => {
    this.subscriptionReconnecting = true
  }
  private readonly handleSubscriptionReady = () => {
    this.subscriptionReconnecting = false
  }
  private readonly handleSubscriptionEnd = () => {
    this.subscriptionEnded = true
  }

  constructor(
    private readonly client: RedisClient,
    options: RedisCoordinatorOptions = {}
  ) {
    this.namespace = options.namespace ?? 'crossflight'
    this.hashKey = options.hashKey ?? defaultHashKey
    this.commandTimeoutMs = options.commandTimeoutMs
    this.subscriptionClient = createSubscriberClient(client)

    this.client.on('error', this.handleCommandError)
    this.client.on('ready', this.handleCommandReady)
    this.client.on('end', this.handleCommandEnd)
    this.subscriptionClient.on('error', this.handleSubscriptionError)
    this.subscriptionClient.on('ready', this.handleSubscriptionReady)
    this.subscriptionClient.on('end', this.handleSubscriptionEnd)
  }

  /**
   * One logical key maps to one `{...}` hash tag, so every Redis key derived
   * from it lands in the same cluster slot and a future multi-key command or
   * Lua script would stay valid on a cluster. The change channel carries the
   * tag for consistency only: a Pub/Sub channel is not a key, no slot routes
   * it, and cluster Pub/Sub reaches every node on its own.
   */
  private resolveLeaseKey(key: string): string {
    return `${this.namespace}:{${this.hashKey(key)}}:flight`
  }

  private resolveChannel(key: string): string {
    return `${this.namespace}:{${this.hashKey(key)}}:change`
  }

  /**
   * A connection is unusable while it is closed, and stays unusable after a
   * connection error only until the client reports itself ready again. A
   * transient error therefore never disables the coordinator permanently.
   */
  private static unavailable(
    status: string,
    ended: boolean,
    reconnecting: boolean
  ): boolean {
    return (
      ended ||
      status === 'close' ||
      status === 'end' ||
      (reconnecting && status !== 'ready')
    )
  }

  private assertOpen(): void {
    if (
      this.closed ||
      RedisCoordinator.unavailable(
        this.client.status,
        this.commandEnded,
        this.commandReconnecting
      )
    ) {
      throw new Error('Redis coordinator is closed')
    }
  }

  private assertSubscribable(): void {
    if (
      this.closed ||
      RedisCoordinator.unavailable(
        this.subscriptionClient.status,
        this.subscriptionEnded,
        this.subscriptionReconnecting
      )
    ) {
      throw new Error('Redis coordinator is closed')
    }
  }

  /** Counts the live waiters per channel: a channel is subscribed for them. */
  private claimChannel(channel: string): void {
    this.channelWaiters.set(
      channel,
      (this.channelWaiters.get(channel) ?? 0) + 1
    )
  }

  /**
   * Releases one waiter's claim. The subscription itself is only dropped once
   * the last waiter leaves and the channel is actually subscribed, so a claim
   * that is still subscribing is never torn down.
   */
  private releaseWaiter(channel: string): void {
    // A claim always exists here: cleanup() only runs for a waiter that claimed,
    // and the entry is only removed in this method.
    const waiters = this.channelWaiters.get(channel)!
    if (waiters > 1) {
      this.channelWaiters.set(channel, waiters - 1)
      return
    }

    this.channelWaiters.delete(channel)
    if (this.subscribedChannels.has(channel)) {
      this.unsubscribeIdleChannel(channel)
    }
  }

  private unsubscribeIdleChannel(channel: string): void {
    if (this.closed) {
      return
    }

    this.subscribedChannels.delete(channel)
    void this.subscriptionClient.unsubscribe(channel).catch(() => undefined)
  }

  /**
   * Translates a connection-level failure into the error the coordinator should
   * surface. Never throws, so it is safe to use inside promise callbacks.
   */
  private coordinatorError(error: unknown): unknown {
    const message = error instanceof Error ? error.message : String(error)
    return isClosedConnectionMessage(message)
      ? new Error('Redis coordinator is closed')
      : error
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
      const result = await withCommandTimeout(
        this.commandTimeoutMs,
        'acquire.eval',
        async () =>
          await this.client.eval(
            `
        return redis.call('set', KEYS[1], ARGV[1], 'PX', ARGV[2], 'NX') ~= false and 1 or 0
        `,
            1,
            baseKey,
            ownerToken,
            String(ttlMs)
          )
      )

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
      throw this.coordinatorError(error)
    }
  }

  async waitForChange(key: string, options?: WaitOptions): Promise<void> {
    this.assertOpen()
    this.assertSubscribable()

    const timeoutMs = options?.timeoutMs ?? 100
    const signal = options?.signal
    const channel = this.resolveChannel(key)

    if (signal?.aborted) {
      throw new DOMException('The operation was aborted', 'AbortError')
    }

    await new Promise<void>((resolve, reject) => {
      let settled = false
      // Claimed before subscribing, so a subscribe that completes after this
      // waiter settled can see that another waiter still needs the channel.
      this.claimChannel(channel)

      const cancelPendingWait = () => {
        cleanup()
        reject(new Error('Redis coordinator is closed'))
      }

      const cleanup = () => {
        if (settled) {
          return
        }

        settled = true
        this.pendingWaits.delete(cancelPendingWait)
        clearTimeout(timer)
        signal?.removeEventListener('abort', onAbort)
        this.subscriptionClient.off('message', onMessage)
        this.releaseWaiter(channel)
      }

      const onAbort = () => {
        cleanup()
        reject(new DOMException('The operation was aborted', 'AbortError'))
      }

      this.pendingWaits.add(cancelPendingWait)

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

      const subscribing = this.subscriptionClient.subscribe(channel)

      // The command can outlive the timeout wrapper below, so the channel is
      // tracked - or dropped again - when the command itself settles.
      void subscribing.then(
        () => {
          if ((this.channelWaiters.get(channel) ?? 0) > 0) {
            this.subscribedChannels.add(channel)
            return
          }

          this.unsubscribeIdleChannel(channel)
        },
        () => undefined
      )

      withCommandTimeout(
        this.commandTimeoutMs,
        'waitForChange.subscribe',
        async () => await subscribing
      ).catch((error) => {
        cleanup()
        reject(this.coordinatorError(error))
      })
    })
  }

  async close(): Promise<void> {
    if (this.closed) {
      return
    }

    this.closed = true

    // Waiters that are still parked must settle: the coordinator is gone.
    for (const cancel of [...this.pendingWaits]) {
      cancel()
    }

    this.client.off('error', this.handleCommandError)
    this.client.off('ready', this.handleCommandReady)
    this.client.off('end', this.handleCommandEnd)
    this.subscriptionClient.off('error', this.handleSubscriptionError)
    this.subscriptionClient.off('ready', this.handleSubscriptionReady)
    this.subscriptionClient.off('end', this.handleSubscriptionEnd)

    for (const channel of [...this.subscribedChannels]) {
      this.subscribedChannels.delete(channel)
      void this.subscriptionClient.unsubscribe(channel).catch(() => undefined)
    }

    try {
      if (
        this.subscriptionClient.status !== 'close' &&
        this.subscriptionClient.status !== 'end'
      ) {
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

/**
 * Works with a single-node `Redis` and with an ioredis `Cluster`: the
 * coordinator only needs single-key commands and pub/sub, and a cluster
 * client routes both on its own.
 */
export function redisCoordinator(
  client: RedisClient,
  options: RedisCoordinatorOptions = {}
): Coordinator {
  return new RedisCoordinator(client, options)
}
