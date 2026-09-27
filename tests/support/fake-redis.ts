import type { RedisClient } from '../../src/coordinators/redis.js'

export interface RecordedEval {
  script: string
  keys: string[]
  args: string[]
}

type Listener = (...args: unknown[]) => void

/**
 * A cluster-shaped stand-in for ioredis: it has `nodes()`, which is what tells
 * the coordinator to build its subscriber from `duplicate()`, so the whole
 * coordinator runs without a socket and every command it issues is recorded
 * instead of sent.
 */
export class FakeRedisClient {
  status = 'ready'
  readonly evals: RecordedEval[] = []
  readonly published: Array<{ channel: string; payload: string }> = []
  readonly subscribed: string[] = []
  readonly unsubscribed: string[] = []
  readonly duplicates: FakeRedisClient[] = []
  private readonly listeners = new Map<string, Set<Listener>>()

  nodes(): string[] {
    return []
  }

  duplicate(): FakeRedisClient {
    const copy = new FakeRedisClient()
    this.duplicates.push(copy)

    return copy
  }

  on(event: string, listener: Listener): this {
    const listeners = this.listeners.get(event) ?? new Set<Listener>()
    listeners.add(listener)
    this.listeners.set(event, listeners)

    return this
  }

  off(event: string, listener: Listener): this {
    this.listeners.get(event)?.delete(listener)

    return this
  }

  /** Every lease operation is one `eval`: the reply drives the whole state machine. */
  async eval(
    script: string,
    numKeys: number,
    ...args: string[]
  ): Promise<number> {
    this.evals.push({
      script,
      keys: args.slice(0, numKeys),
      args: args.slice(numKeys),
    })

    return 1
  }

  async publish(channel: string, payload: string): Promise<number> {
    this.published.push({ channel, payload })

    return 1
  }

  async subscribe(channel: string): Promise<void> {
    this.subscribed.push(channel)
  }

  async unsubscribe(channel: string): Promise<void> {
    this.unsubscribed.push(channel)
  }

  async quit(): Promise<'OK'> {
    return 'OK'
  }

  asClient(): RedisClient {
    return this as unknown as RedisClient
  }
}
