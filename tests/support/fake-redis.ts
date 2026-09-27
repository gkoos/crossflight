import type { RedisClient } from '../../src/coordinators/redis.js'

export interface RecordedEval {
  script: string
  keys: string[]
  args: string[]
}

type Listener = (...args: unknown[]) => void

/** What a scripted `eval` replies with: a value, a failure, or a handler. */
export type EvalReply =
  number | Error | ((call: RecordedEval) => number | Promise<number>)

/** What a scripted `publish` replies with. A handler can park the command. */
export type PublishReply =
  | number
  | Error
  | ((channel: string, payload: string) => number | Promise<number>)

/**
 * What a scripted `subscribe` replies with. The subscription is recorded and
 * delivered to when the command is issued; the reply only decides whether the
 * command succeeded - a handler that parks models a socket that never answers.
 */
export type SubscribeReply =
  number | Error | ((channel: string) => number | Promise<number>)

/**
 * Connects fakes the way a Redis server connects clients: a message reaches
 * every client subscribed to the channel. Two coordinators built on two clients
 * of one hub therefore wake each other - which is what the wake-up half of the
 * contract is about - without a socket.
 */
export class FakeRedisHub {
  private readonly subscribers = new Map<string, Set<FakeRedisClient>>()

  /** Delivers the message to the channel's subscribers and reports how many. */
  publish(channel: string, payload: string): number {
    const subscribers = [...(this.subscribers.get(channel) ?? [])]

    for (const client of subscribers) {
      client.deliver(channel, payload)
    }

    return subscribers.length
  }

  claim(client: FakeRedisClient, channel: string): void {
    const subscribers =
      this.subscribers.get(channel) ?? new Set<FakeRedisClient>()

    subscribers.add(client)
    this.subscribers.set(channel, subscribers)
  }

  release(client: FakeRedisClient, channel: string): void {
    const subscribers = this.subscribers.get(channel)

    subscribers?.delete(client)

    if (subscribers?.size === 0) {
      this.subscribers.delete(channel)
    }
  }
}

/**
 * A cluster-shaped stand-in for ioredis: it has `nodes()`, which is what tells
 * the coordinator to build its subscriber from `duplicate()`, so the whole
 * coordinator runs without a socket and every command it issues is recorded
 * instead of sent.
 *
 * A test can script the connection - the reply of an `eval`, a `publish` that
 * fails, a command that never answers - and drive the events ioredis would
 * report: a status change, or a message pushed to a subscriber. Nothing here
 * decides what the coordinator does; the script is the environment it has to
 * cope with.
 */
export class FakeRedisClient {
  status = 'ready'
  readonly evals: RecordedEval[] = []
  readonly published: Array<{ channel: string; payload: string }> = []
  readonly subscribed: string[] = []
  readonly unsubscribed: string[] = []
  readonly duplicates: FakeRedisClient[] = []
  /** How often the coordinator closed this connection. */
  quitCalls = 0
  private readonly listeners = new Map<string, Set<Listener>>()
  private evalReply: EvalReply = 1
  private readonly queuedEvalReplies: EvalReply[] = []
  private publishReply: PublishReply = 1
  private subscribeReply: SubscribeReply = 1

  constructor(private readonly hub?: FakeRedisHub) {}

  nodes(): string[] {
    return []
  }

  duplicate(): FakeRedisClient {
    const copy = new FakeRedisClient(this.hub)
    this.duplicates.push(copy)

    return copy
  }

  /** Every later `eval` replies this, until it is replaced. */
  replyToEval(reply: EvalReply): void {
    this.evalReply = reply
  }

  /**
   * The next `eval` replies this once and then falls back to the default. What a
   * lease operation does is decided by the reply of the script it runs, so a
   * takeover is driven by the sequence.
   */
  queueEvalReply(reply: EvalReply): void {
    this.queuedEvalReplies.push(reply)
  }

  /** Every later `publish` replies this. An `Error` reaches no subscriber. */
  replyToPublish(reply: PublishReply): void {
    this.publishReply = reply
  }

  /** Every later `subscribe` replies this. */
  replyToSubscribe(reply: SubscribeReply): void {
    this.subscribeReply = reply
  }

  /**
   * Moves the connection to a status and reports the event ioredis reports with
   * it: `ready` and `end` are the events the coordinator watches for, and any
   * other status arrives as an `error` on a connection that is not ready.
   */
  setStatus(status: string): void {
    this.status = status

    if (status === 'ready') {
      this.emit('ready')
      return
    }

    if (status === 'end') {
      this.emit('end')
      return
    }

    this.emit('error', new Error(`Connection ${status}`))
  }

  /** Pushes a message to this connection, as a subscriber receives one. */
  deliver(channel: string, payload = '1'): void {
    this.emit('message', channel, payload)
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

  /** Reports an event to this connection's listeners, as ioredis would. */
  emit(event: string, ...args: unknown[]): void {
    for (const listener of [...(this.listeners.get(event) ?? [])]) {
      listener(...args)
    }
  }

  /** Every lease operation is one `eval`: the reply drives the state machine. */
  async eval(
    script: string,
    numKeys: number,
    ...args: string[]
  ): Promise<number> {
    const call: RecordedEval = {
      script,
      keys: args.slice(0, numKeys),
      args: args.slice(numKeys),
    }

    this.evals.push(call)

    return await this.evalResult(
      this.queuedEvalReplies.shift() ?? this.evalReply,
      call
    )
  }

  async publish(channel: string, payload: string): Promise<number> {
    this.published.push({ channel, payload })

    // Thrown here means the message reached nobody: a failed publish is exactly
    // the case an ownership change has to survive.
    const reply = await this.publishResult(this.publishReply, channel, payload)

    this.hub?.publish(channel, payload)

    return reply
  }

  async subscribe(channel: string): Promise<void> {
    // Recorded when the command is issued, so a message published right
    // afterwards is delivered: the reply below only decides whether the
    // subscribe succeeded.
    this.subscribed.push(channel)
    this.hub?.claim(this, channel)

    const settled =
      typeof this.subscribeReply === 'function'
        ? await this.subscribeReply(channel)
        : this.subscribeReply

    if (settled instanceof Error) {
      throw settled
    }
  }

  async unsubscribe(channel: string): Promise<void> {
    this.unsubscribed.push(channel)
    this.hub?.release(this, channel)
  }

  async quit(): Promise<'OK'> {
    this.quitCalls += 1

    return 'OK'
  }

  asClient(): RedisClient {
    return this as unknown as RedisClient
  }

  private async evalResult(
    reply: EvalReply,
    call: RecordedEval
  ): Promise<number> {
    const settled = typeof reply === 'function' ? await reply(call) : reply

    if (settled instanceof Error) {
      throw settled
    }

    return settled
  }

  private async publishResult(
    reply: PublishReply,
    channel: string,
    payload: string
  ): Promise<number> {
    const settled =
      typeof reply === 'function' ? await reply(channel, payload) : reply

    if (settled instanceof Error) {
      throw settled
    }

    return settled
  }
}
