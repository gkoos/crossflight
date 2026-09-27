import * as fc from 'fast-check'
import { describe, expect } from 'vitest'

import {
  RedisCommandTimeoutError,
  redisCoordinator,
  type RedisCoordinatorOptions,
} from '../../src/coordinators/redis.js'
import type { Coordinator } from '../../src/types.js'
import { FakeRedisClient, FakeRedisHub } from '../support/fake-redis.js'
import { createPropertySuite } from '../support/seed.js'

/**
 * The half of the Redis coordinator that a key layout cannot cover: waiting for
 * a change, the command timeout that bounds a socket that never answers, and the
 * connection events a real client reports. All of it runs against a scripted
 * fake, so a test decides what the socket does and the coordinator has to cope
 * with it:
 *
 * - a change announced on another instance wakes a waiter, and a message for
 *   another key does not;
 * - a channel is subscribed for the waiters that need it, and only dropped when
 *   the last of them leaves;
 * - a waiter parked on a coordinator that closes is settled, and the closed
 *   coordinator neither serves nor commands afterwards;
 * - a transient connection error is survivable, a terminal `end` is not, and an
 *   unavailable connection is never asked at all;
 * - every command that can hang is bounded, including the one a waiter issues;
 * - a wake-up that cannot be delivered is only an optimisation: the lease
 *   operation it announced still succeeds, and the waiter falls back to its own
 *   timeout.
 */
const itProperty = createPropertySuite('redis-waits', { runs: 40 })

const CLOSED = 'Redis coordinator is closed'

const KEY = 'waits:key'

/** The keys a case uses, small enough that two of them meet in one channel set. */
const key: fc.Arbitrary<string> = fc.oneof(
  fc.constantFrom(KEY, 'waits:other'),
  fc.string({ minLength: 1, maxLength: 8 })
)

const tick = (): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, 0))

/** A command a handler can park: it never settles, and never keeps a process up. */
const parked = <T>(): Promise<T> => new Promise<T>(() => {})

interface Session {
  /** The connection the coordinator issues commands on. */
  command: FakeRedisClient
  /** The second connection it built for its subscription. */
  subscription: FakeRedisClient
  coordinator: Coordinator
}

const session = (
  options: RedisCoordinatorOptions = {},
  hub?: FakeRedisHub
): Session => {
  const command = new FakeRedisClient(hub)
  const coordinator = redisCoordinator(command.asClient(), options)

  // A cluster-shaped client is duplicated, so the subscriber is the first
  // duplicate: the same connection ioredis would hand the coordinator.
  return { command, subscription: command.duplicates[0]!, coordinator }
}

interface Attempt {
  ms: number
  value: unknown
  error: unknown
}

/** Runs one call and reports how long it took and how it settled. */
const timed = async (work: () => Promise<unknown>): Promise<Attempt> => {
  const startedAt = Date.now()

  try {
    const value = await work()

    return { ms: Date.now() - startedAt, value, error: undefined }
  } catch (error) {
    return { ms: Date.now() - startedAt, value: undefined, error }
  }
}

/** The generated order, as a permutation of `count` indexes. */
const permutation = (order: number[], count: number): number[] => {
  const chosen: number[] = []

  for (const value of order) {
    const index = value % count

    if (!chosen.includes(index)) {
      chosen.push(index)
    }
  }

  for (let index = 0; index < count; index += 1) {
    if (!chosen.includes(index)) {
      chosen.push(index)
    }
  }

  return chosen
}

type Transition = 'error' | 'ready' | 'end'

const transition: fc.Arbitrary<Transition> = fc.constantFrom(
  'error' as const,
  'ready' as const,
  'end' as const
)

/**
 * The connection state the coordinator keeps, from the events alone: an `error`
 * marks the connection as reconnecting, `ready` clears it, and `end` is the one
 * event that never comes back.
 */
const connectionModel = () => {
  let status = 'ready'
  let ended = false
  let reconnecting = false

  return {
    apply(next: Transition): void {
      if (next === 'ready') {
        status = 'ready'
        reconnecting = false
        return
      }

      if (next === 'end') {
        status = 'end'
        ended = true
        return
      }

      status = 'reconnecting'
      reconnecting = true
    },
    available(): boolean {
      return (
        !ended &&
        status !== 'close' &&
        status !== 'end' &&
        !(reconnecting && status !== 'ready')
      )
    },
  }
}

describe('the redis coordinator waiting for a change', () => {
  itProperty(
    'wakes a waiter on another instance whenever the lease changes',
    fc.record({
      key,
      mutation: fc.constantFrom(
        'renew' as const,
        'complete' as const,
        'abandon' as const
      ),
    }),
    async ({ key, mutation }) => {
      const hub = new FakeRedisHub()
      const owner = session({}, hub)
      const waiter = session({}, hub)

      try {
        const lease = await owner.coordinator.acquire(key, { ttlMs: 1000 })
        expect(lease).not.toBeNull()

        // The acquisition is announced too, which is why the waiter below is
        // started after it: only the mutation may settle this wait.
        const channel = owner.command.published[0]!.channel
        const waiting = waiter.coordinator.waitForChange(key, {
          timeoutMs: 5000,
        })

        await (mutation === 'renew'
          ? lease!.renew()
          : mutation === 'complete'
            ? lease!.complete()
            : lease!.abandon())

        await expect(waiting).resolves.toBeUndefined()

        // The waiter's connection was subscribed for it, and dropped again once
        // it was the last one to leave.
        expect(waiter.subscription.subscribed).toEqual([channel])
        expect(waiter.subscription.unsubscribed).toEqual([channel])

        // An owner announces a change on the connection it mutates through, and
        // never subscribes to hear itself.
        expect(owner.command.published).toHaveLength(2)
        expect(owner.subscription.subscribed).toEqual([])
      } finally {
        await Promise.all([
          owner.coordinator.close(),
          waiter.coordinator.close(),
        ])
      }
    }
  )

  itProperty('is not woken by a message for another key', key, async (key) => {
    const { command, subscription, coordinator } = session()

    try {
      await coordinator.acquire(key, { ttlMs: 1000 })
      // A second key announces on a channel of its own, which is what a
      // message that must not wake this waiter looks like.
      await coordinator.acquire(`${key}!`, { ttlMs: 1000 })

      const ownChannel = command.published[0]!.channel
      const otherChannel = command.published[1]!.channel
      expect(otherChannel).not.toBe(ownChannel)

      const waiting = coordinator.waitForChange(key, { timeoutMs: 5000 })
      let settled = false
      waiting.then(
        () => {
          settled = true
        },
        () => {
          settled = true
        }
      )

      // Pushed to the very connection that is subscribed for this waiter, so
      // only the coordinator's own filtering can keep it parked.
      subscription.deliver(otherChannel, '1')
      await tick()
      expect(settled).toBe(false)

      subscription.deliver(ownChannel, '1')
      await expect(waiting).resolves.toBeUndefined()

      expect(subscription.subscribed).toEqual([ownChannel])
    } finally {
      await coordinator.close()
    }
  })

  itProperty(
    'subscribes once per waiter and drops the channel only with the last one',
    fc.record({
      waiters: fc.integer({ min: 2, max: 4 }),
      order: fc.array(fc.nat({ max: 20 }), { minLength: 1, maxLength: 4 }),
    }),
    async ({ waiters, order }) => {
      const { subscription, coordinator } = session()

      try {
        const controllers = Array.from(
          { length: waiters },
          () => new AbortController()
        )
        const outcomes = controllers.map(async (controller) => {
          try {
            await coordinator.waitForChange(KEY, {
              timeoutMs: 5000,
              signal: controller.signal,
            })
            return undefined
          } catch (error) {
            return error
          }
        })

        const channel = subscription.subscribed[0]!
        expect(subscription.subscribed).toHaveLength(waiters)
        expect(subscription.subscribed.every((seen) => seen === channel)).toBe(
          true
        )

        let remaining = waiters

        for (const index of permutation(order, waiters)) {
          controllers[index]!.abort()
          remaining -= 1
          await tick()

          // A waiter leaving is not the channel leaving: it stays subscribed
          // for the ones still parked on it.
          expect(subscription.unsubscribed, `${remaining} left`).toEqual(
            remaining === 0 ? [channel] : []
          )
        }

        for (const outcome of await Promise.all(outcomes)) {
          expect(outcome).toBeInstanceOf(DOMException)
          expect((outcome as DOMException).name).toBe('AbortError')
        }
      } finally {
        await coordinator.close()
      }
    },
    { runs: 20 }
  )

  itProperty(
    'never touches the connection for a signal that has already aborted',
    key,
    async (key) => {
      const { command, subscription, coordinator } = session()
      const controller = new AbortController()
      controller.abort()

      try {
        await expect(
          coordinator.waitForChange(key, { signal: controller.signal })
        ).rejects.toBeInstanceOf(DOMException)

        expect(subscription.subscribed).toEqual([])
        expect(subscription.unsubscribed).toEqual([])
        expect(command.evals).toEqual([])
      } finally {
        await coordinator.close()
      }
    }
  )

  itProperty(
    'settles every parked waiter when it is closed, and serves nothing after',
    fc.record({
      waiters: fc.integer({ min: 1, max: 3 }),
      /** Whether the subscribe landed before the close did. */
      subscribeLands: fc.boolean(),
    }),
    async ({ waiters, subscribeLands }) => {
      const { command, subscription, coordinator } = session()
      const outcomes = Array.from({ length: waiters }, async () => {
        try {
          await coordinator.waitForChange(KEY, { timeoutMs: 5000 })
          return undefined
        } catch (error) {
          return error
        }
      })

      const channel = subscription.subscribed[0]!

      if (subscribeLands) {
        await tick()
        await coordinator.close()

        // The channel was subscribed when the close ran, so it is the
        // coordinator's to drop.
        expect(subscription.unsubscribed).toContain(channel)
      } else {
        // A close that races the subscribe never learns of the channel and
        // hands it to the connection teardown instead - which is what a real
        // `QUIT` does with every subscription on the socket.
        await coordinator.close()
        expect(subscription.subscribed).toContain(channel)
      }

      expect(subscription.quitCalls).toBe(1)

      for (const error of await Promise.all(outcomes)) {
        expect(error).toBeInstanceOf(Error)
        expect((error as Error).message).toBe(CLOSED)
      }

      // Closed means closed: no command is issued, and closing again is a no-op.
      const evals = command.evals.length
      await expect(coordinator.acquire(KEY, { ttlMs: 50 })).rejects.toThrow(
        CLOSED
      )
      await expect(
        coordinator.waitForChange(KEY, { timeoutMs: 5 })
      ).rejects.toThrow(CLOSED)
      expect(command.evals).toHaveLength(evals)
      await expect(coordinator.close()).resolves.toBeUndefined()
      expect(subscription.quitCalls).toBe(1)
    }
  )
})

describe('the redis coordinator facing a connection', () => {
  itProperty(
    'survives a transient error, gives up on an end, and never asks a dead socket',
    fc.record({
      command: fc.array(transition, { minLength: 1, maxLength: 5 }),
      subscription: fc.array(transition, { minLength: 1, maxLength: 4 }),
    }),
    async ({ command: events, subscription: subscriptionEvents }) => {
      const { command, subscription, coordinator } = session()
      const commandModel = connectionModel()
      const subscriptionModel = connectionModel()

      try {
        for (const next of events) {
          command.setStatus(next)
          commandModel.apply(next)

          const evals = command.evals.length
          const attempt = await timed(() =>
            coordinator.acquire(KEY, { ttlMs: 50 })
          )

          if (commandModel.available()) {
            expect(attempt.error).toBeUndefined()
            // One command for one acquisition, and nothing spent on finding out
            // whether the connection is up.
            expect(command.evals.length).toBe(evals + 1)
          } else {
            expect(attempt.error).toBeInstanceOf(Error)
            expect((attempt.error as Error).message).toBe(CLOSED)
            expect(command.evals.length).toBe(evals)
          }
        }

        for (const next of subscriptionEvents) {
          subscription.setStatus(next)
          subscriptionModel.apply(next)

          const attempt = await timed(() =>
            coordinator.waitForChange(KEY, { timeoutMs: 5 })
          )

          if (commandModel.available() && subscriptionModel.available()) {
            expect(attempt.error).toBeUndefined()
          } else {
            // The command connection is checked first, so a waiter on a broken
            // subscriber fails for the same reason a command would.
            expect(attempt.error).toBeInstanceOf(Error)
            expect((attempt.error as Error).message).toBe(CLOSED)
          }
        }

        // However the connection ended, closing has to be able to finish.
        await expect(coordinator.close()).resolves.toBeUndefined()
      } catch (error) {
        await coordinator.close().catch(() => undefined)
        throw error
      }
    }
  )

  itProperty(
    'bounds a lease command that never answers by commandTimeoutMs',
    fc.record({
      operation: fc.constantFrom(
        'acquire' as const,
        'renew' as const,
        'complete' as const,
        'abandon' as const
      ),
      timeoutMs: fc.constantFrom(10, 20, 40),
    }),
    async ({ operation, timeoutMs }) => {
      const { command, coordinator } = session({ commandTimeoutMs: timeoutMs })

      try {
        if (operation === 'acquire') {
          command.replyToEval(() => parked<number>())

          const attempt = await timed(() =>
            coordinator.acquire(KEY, { ttlMs: 50 })
          )

          expect(attempt.error).toBeInstanceOf(RedisCommandTimeoutError)
          expect((attempt.error as Error).message).toContain('acquire.eval')
          expect((attempt.error as Error).message).toContain(`${timeoutMs}ms`)
          expect(attempt.ms).toBeGreaterThanOrEqual(timeoutMs - 5)
          return
        }

        const lease = await coordinator.acquire(KEY, { ttlMs: 50 })
        expect(lease).not.toBeNull()

        // The lease is in hand, so the parked command is the one under test.
        command.replyToEval(() => parked<number>())

        const attempt = await timed(() =>
          operation === 'renew'
            ? lease!.renew()
            : operation === 'complete'
              ? lease!.complete()
              : lease!.abandon()
        )

        expect(attempt.error).toBeInstanceOf(RedisCommandTimeoutError)
        expect((attempt.error as Error).message).toContain(
          `lease.${operation}.eval`
        )
        expect(attempt.ms).toBeGreaterThanOrEqual(timeoutMs - 5)
      } finally {
        await coordinator.close()
      }
    },
    { runs: 20 }
  )

  itProperty(
    'bounds a subscribe that never answers by commandTimeoutMs',
    fc.constantFrom(10, 20, 40),
    async (timeoutMs) => {
      const { subscription, coordinator } = session({
        commandTimeoutMs: timeoutMs,
      })

      // A socket that accepts the command and never replies: the wait has to be
      // bounded even though the waiter's own timeout is far away.
      subscription.replyToSubscribe(() => parked<number>())

      try {
        const attempt = await timed(() =>
          coordinator.waitForChange(KEY, { timeoutMs: 5000 })
        )

        expect(attempt.error).toBeInstanceOf(RedisCommandTimeoutError)
        expect((attempt.error as Error).message).toContain(
          'waitForChange.subscribe'
        )
        expect(attempt.ms).toBeGreaterThanOrEqual(timeoutMs - 5)
      } finally {
        await coordinator.close()
      }
    },
    { runs: 20 }
  )

  itProperty(
    'survives a publish that fails, because a wake-up is only an optimisation',
    fc.constantFrom(
      'acquire' as const,
      'renew' as const,
      'complete' as const,
      'abandon' as const
    ),
    async (operation) => {
      const { command, coordinator } = session()

      try {
        const lease = await coordinator.acquire(KEY, { ttlMs: 50 })
        expect(lease).not.toBeNull()

        command.replyToPublish(new Error('publish failed'))

        if (operation === 'renew') {
          await expect(lease!.renew()).resolves.toBe(true)
        } else if (operation === 'complete') {
          await expect(lease!.complete()).resolves.toBeUndefined()
        } else if (operation === 'abandon') {
          await expect(lease!.abandon()).resolves.toBeUndefined()
        } else {
          // An acquisition announces itself on the way out: a failed announce
          // must not turn a taken lease into a refusal.
          await expect(
            coordinator.acquire(`${KEY}:second`, { ttlMs: 50 })
          ).resolves.toBeTruthy()
        }

        // It was attempted, and it failed: that is the whole point.
        expect(command.published.length).toBeGreaterThan(0)
      } finally {
        await coordinator.close()
      }
    }
  )

  itProperty(
    'leaves a waiter to its own timeout when the change cannot be announced',
    fc.constantFrom(5, 15),
    async (timeoutMs) => {
      const hub = new FakeRedisHub()
      const owner = session({}, hub)
      const waiter = session({}, hub)

      try {
        const lease = await owner.coordinator.acquire(KEY, { ttlMs: 1000 })
        expect(lease).not.toBeNull()
        owner.command.replyToPublish(new Error('publish failed'))

        const attempt = await timed(async () => {
          const waiting = waiter.coordinator.waitForChange(KEY, { timeoutMs })
          await tick()
          await lease!.complete()
          await waiting
        })

        // No wake-up arrived and the wait still settled: a lost notification is
        // a delay, never a hang.
        expect(attempt.error).toBeUndefined()
        expect(attempt.ms).toBeGreaterThanOrEqual(timeoutMs - 5)
      } finally {
        await Promise.all([
          owner.coordinator.close(),
          waiter.coordinator.close(),
        ])
      }
    }
  )

  itProperty(
    'takes the lease exactly when the script reports it did',
    fc.boolean(),
    async (granted) => {
      const { command, coordinator } = session()
      command.replyToEval(granted ? 1 : 0)

      try {
        const lease = await coordinator.acquire(KEY, { ttlMs: 50 })

        expect(lease === null).toBe(!granted)
        // A refusal is not a change: nothing is announced for a key this
        // process does not own.
        expect(command.published).toHaveLength(granted ? 1 : 0)
      } finally {
        await coordinator.close()
      }
    }
  )

  itProperty(
    'reports a takeover as a renewal that stops holding, and stops announcing',
    fc.integer({ min: 0, max: 1 }),
    async (takeoverAt) => {
      const { command, coordinator } = session()

      try {
        const lease = await coordinator.acquire(KEY, { ttlMs: 50 })
        expect(lease).not.toBeNull()

        // The script keeps the lease once and then reports the key as someone
        // else's: which renewal loses it is the script's choice, and the token
        // is what decides.
        command.queueEvalReply(takeoverAt === 0 ? 0 : 1)
        command.queueEvalReply(takeoverAt === 1 ? 0 : 1)

        const renewals = [await lease!.renew(), await lease!.renew()]

        expect(renewals).toEqual(
          takeoverAt === 0 ? [false, true] : [true, false]
        )
        // One acquisition and the one renewal that held: the renewal that lost
        // the key announces nothing.
        expect(command.published).toHaveLength(2)
        expect(command.evals).toHaveLength(3)
      } finally {
        await coordinator.close()
      }
    }
  )
})
