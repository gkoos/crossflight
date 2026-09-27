import * as fc from 'fast-check'
import { describe, expect } from 'vitest'

import { createCrossflight } from '../../src/index.js'
import type {
  CacheAdapter,
  CoordinationFailureMode,
  Coordinator,
  CrossflightEvent,
} from '../../src/types.js'
import { InMemoryCoordinator } from '../mocks/in-memory-coordinator.js'
import { memoryStore, type StoreHarness } from '../support/cache-store.js'
import { createPropertySuite } from '../support/seed.js'

/**
 * `createCrossflight`'s in-process guarantees are about *arrivals*: who was
 * waiting when the loader started, when it settled and which of them cancelled.
 * The unit suite pins those cases one at a time; the properties here generate
 * the patterns instead - a loader parked until the case releases it, and a
 * number of callers that join, cancel and observe - and assert what has to hold
 * for every pattern:
 *
 * - one loader run serves every caller that joined the flight. The loader's
 *   result carries its run number, so a caller that ran a loader of its own is
 *   visible rather than plausible;
 * - a caller observes its value only after the store was handed it;
 * - coalescing is per flight, not permanent: with the value gone from the store
 *   the next call loads again;
 * - a cancellation rejects that caller alone; only the last caller to leave
 *   takes the flight with it, and no lease is left behind either way;
 * - the events a flight reports are a function of its arrivals.
 */
const itProperty = createPropertySuite('flight', { runs: 60 })

const KEY = 'flight:key'

/**
 * `defaultTtlMs` also bounds the coordination lease (floored at
 * `MIN_LEASE_TTL_MS`), and it is chosen so the renewal keeper never fires inside
 * a case: these properties are about callers and their flight, not about lease
 * maintenance, which `lease-keeper.test.ts` and the coordinator suites cover.
 */
const DEFAULT_TTL_MS = 500

/** Every event that means the flight did not simply serve its callers. */
const FAILURE_EVENTS = ['failed', 'fallback', 'wait_exhausted'] as const

const tick = (): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, 0))

interface Deferred {
  promise: Promise<void>
  resolve: () => void
}

const deferred = (): Deferred => {
  let resolve!: () => void
  const promise = new Promise<void>((res) => {
    resolve = res
  })

  return { promise, resolve }
}

/**
 * Yields until the predicate holds. Cleanup that a settled flight starts in the
 * background - releasing its lease, for one - has to be observed rather than
 * assumed, so the assertion is on the predicate's final state and never on a
 * fixed number of ticks.
 */
const settles = async (
  predicate: () => boolean,
  timeoutMs = 1000
): Promise<boolean> => {
  const deadline = Date.now() + timeoutMs

  while (!predicate() && Date.now() < deadline) {
    await tick()
  }

  return predicate()
}

const typesOf = (events: CrossflightEvent[]): string[] =>
  events.map((event) => event.type)

const countOf = (events: CrossflightEvent[], type: string): number =>
  typesOf(events).filter((seen) => seen === type).length

const nullCount = (events: CrossflightEvent[]): void => {
  expect(
    typesOf(events).filter((seen) => FAILURE_EVENTS.includes(seen as never))
  ).toEqual([])
}

interface Harness {
  store: ReturnType<typeof memoryStore>
  coordinator: InMemoryCoordinator
  events: CrossflightEvent[]
  crossflight: ReturnType<typeof createCrossflight>
}

const harness = (
  options: {
    store?: StoreHarness
    adapter?: CacheAdapter
    coordinator?: Coordinator
    failureMode?: CoordinationFailureMode
  } = {}
): Harness => {
  const store = options.store ?? memoryStore()
  const coordinator = new InMemoryCoordinator()
  const events: CrossflightEvent[] = []
  const crossflight = createCrossflight({
    cache: options.adapter ?? store.adapter,
    coordinator: options.coordinator ?? coordinator,
    defaultTtlMs: DEFAULT_TTL_MS,
    failureMode: options.failureMode,
    onEvent: (event) => events.push(event),
  })

  return { store, coordinator, events, crossflight }
}

describe('a shared flight', () => {
  itProperty(
    'serves every caller that joined it from one loader run',
    fc.record({
      value: fc.jsonValue(),
      callers: fc.integer({ min: 1, max: 8 }),
    }),
    async ({ value, callers }) => {
      const { store, coordinator, events, crossflight } = harness()

      try {
        const gate = deferred()
        let runs = 0
        const loader = async (): Promise<{ run: number; value: unknown }> => {
          runs += 1
          await gate.promise
          return { run: runs, value }
        }

        const owner = crossflight.wrap(KEY, loader)
        // The flight has to exist, and has to have decided it is a miss, before
        // the joiners arrive: a macrotask settles the cache read that decides.
        await tick()
        const joiners = Array.from({ length: callers - 1 }, () =>
          crossflight.wrap(KEY, loader)
        )
        await tick()

        expect(runs).toBe(1)

        gate.resolve()
        const results = await Promise.all([owner, ...joiners])

        // Every caller observed the one run: a joiner that had started a loader
        // of its own would report run 2.
        expect(results).toHaveLength(callers)
        expect(results.every((result) => result.run === 1)).toBe(true)
        expect(results.every((result) => Object.is(result.value, value))).toBe(
          true
        )
        expect(runs).toBe(1)
        expect(store.writes).toHaveLength(1)
        expect(store.writes[0]!.value).toEqual({ run: 1, value })

        // The events are a function of the arrivals: one miss, one acquisition,
        // one completion, and one local join per caller that arrived late.
        expect(typesOf(events)[0]).toBe('miss')
        expect(typesOf(events).at(-1)).toBe('completed')
        expect(countOf(events, 'miss')).toBe(1)
        expect(countOf(events, 'ownership_acquired')).toBe(1)
        expect(countOf(events, 'local_join')).toBe(callers - 1)
        expect(countOf(events, 'completed')).toBe(1)
        nullCount(events)

        expect(await settles(() => coordinator.owners.size === 0)).toBe(true)
      } finally {
        await crossflight.close()
      }
    }
  )

  itProperty(
    'hands the store the value before any of its callers observes it',
    fc.record({
      value: fc.jsonValue(),
      callers: fc.integer({ min: 1, max: 5 }),
      ttl: fc.option(fc.integer({ min: 0, max: 600_000 }), { nil: undefined }),
    }),
    async ({ value, callers, ttl }) => {
      const store = memoryStore()
      const order: string[] = []
      const adapter: CacheAdapter = {
        get: (key) => store.adapter.get(key),
        set: async (key, stored, options) => {
          order.push('store')
          await store.adapter.set(key, stored, options)
        },
      }
      const { coordinator, events, crossflight } = harness({ store, adapter })

      try {
        const gate = deferred()
        let runs = 0
        const loader = async (): Promise<{ run: number; value: unknown }> => {
          runs += 1
          await gate.promise
          return { run: runs, value }
        }

        const observed = (
          promise: Promise<{ run: number; value: unknown }>,
          caller: number
        ): Promise<{ run: number; value: unknown }> =>
          promise.then((result) => {
            order.push(`observe:${caller}`)
            return result
          })

        const owner = observed(crossflight.wrap(KEY, loader, { ttl }), 0)
        await tick()
        const joiners = Array.from({ length: callers - 1 }, (_, index) =>
          observed(crossflight.wrap(KEY, loader, { ttl }), index + 1)
        )
        await tick()

        gate.resolve()
        await Promise.all([owner, ...joiners])

        expect(order[0]).toBe('store')
        expect(order.filter((entry) => entry === 'store')).toHaveLength(1)
        expect(order).toHaveLength(callers + 1)

        // One write, and the caller's ttl is what the store was handed: the
        // value's lifetime is the caller's, not the lease's.
        expect(store.writes).toHaveLength(1)
        expect(store.writes[0]!.options).toEqual({ ttl })
        expect(countOf(events, 'completed')).toBe(1)
        expect(await settles(() => coordinator.owners.size === 0)).toBe(true)
      } finally {
        await crossflight.close()
      }
    }
  )

  itProperty(
    'coalesces per flight rather than for the lifetime of the key',
    fc.record({
      first: fc.jsonValue(),
      second: fc.jsonValue(),
      callers: fc.integer({ min: 1, max: 4 }),
    }),
    async ({ first, second, callers }) => {
      const { store, coordinator, crossflight } = harness()

      try {
        let runs = 0
        const flight = async (
          value: unknown
        ): Promise<Array<{ run: number; value: unknown }>> => {
          const gate = deferred()
          const loader = async (): Promise<{ run: number; value: unknown }> => {
            runs += 1
            await gate.promise
            return { run: runs, value }
          }

          const owner = crossflight.wrap(KEY, loader)
          await tick()
          const joiners = Array.from({ length: callers - 1 }, () =>
            crossflight.wrap(KEY, loader)
          )
          await tick()

          gate.resolve()
          return Promise.all([owner, ...joiners])
        }

        const firstFlight = await flight(first)
        expect(firstFlight.every((result) => result.run === 1)).toBe(true)
        expect(
          firstFlight.every((result) => Object.is(result.value, first))
        ).toBe(true)

        // What an eviction, a ttl or another writer does to the store: a flight
        // record that outlived the value it published would hand the next
        // caller a value the cache no longer has.
        store.forget(KEY)

        const secondFlight = await flight(second)
        expect(secondFlight.every((result) => result.run === 2)).toBe(true)
        expect(
          secondFlight.every((result) => Object.is(result.value, second))
        ).toBe(true)
        expect(store.writes.map((write) => write.value)).toEqual([
          { run: 1, value: first },
          { run: 2, value: second },
        ])
        expect(runs).toBe(2)
        expect(await settles(() => coordinator.owners.size === 0)).toBe(true)
      } finally {
        await crossflight.close()
      }
    }
  )

  itProperty(
    'rejects only the caller that cancelled and serves the rest',
    fc.record({
      value: fc.jsonValue(),
      callers: fc.integer({ min: 1, max: 6 }),
      cancelCount: fc.integer({ min: 0, max: 6 }),
      fromEnd: fc.boolean(),
    }),
    async ({ value, callers, cancelCount, fromEnd }) => {
      const { store, coordinator, events, crossflight } = harness()

      try {
        const cancelling = Math.min(cancelCount, callers)
        const reasons = Array.from(
          { length: callers },
          (_, index) => new Error(`caller ${index} cancelled`)
        )
        const controllers = reasons.map(() => new AbortController())
        // Who cancels: the earliest arrivals or the latest ones, so both the
        // caller that owns the attempt and a joiner get cancelled by cases.
        const cancelled = new Set(
          Array.from({ length: cancelling }, (_, offset) =>
            fromEnd ? callers - 1 - offset : offset
          )
        )

        const gate = deferred()
        const finished = deferred()
        let runs = 0
        let sawAbort = false
        const loader = async (
          signal: AbortSignal
        ): Promise<{ run: number; value: unknown }> => {
          runs += 1
          await gate.promise
          sawAbort = signal.aborted
          finished.resolve()
          return { run: runs, value }
        }

        const calls = reasons.map((_, index) => {
          const controller = controllers[index]!
          return cancelled.has(index)
            ? crossflight.wrap(KEY, loader, { signal: controller.signal })
            : crossflight.wrap(KEY, loader)
        })

        await tick()
        expect(runs).toBe(1)

        // Handlers first: the aborts below reject a caller synchronously, and a
        // rejection nobody is attached to yet is reported as an unhandled one.
        const allSettled = Promise.allSettled(calls)

        for (const index of cancelled) {
          controllers[index]!.abort(reasons[index])
        }

        await tick()
        gate.resolve()
        const settled = await allSettled
        await finished.promise

        settled.forEach((outcome, index) => {
          if (cancelled.has(index)) {
            // Its own reason, by identity - never the flight's, and never
            // another caller's.
            expect(outcome.status).toBe('rejected')
            if (outcome.status === 'rejected') {
              expect(outcome.reason).toBe(reasons[index])
            }
            return
          }

          expect(outcome.status).toBe('fulfilled')
          if (outcome.status === 'fulfilled') {
            expect(outcome.value.run).toBe(1)
          }
        })

        expect(runs).toBe(1)
        expect(countOf(events, 'cancelled')).toBe(cancelling)
        // The flight dies with its last caller: a loader still parked observes
        // the abort and nothing is published. With a survivor it continues, and
        // publishes once.
        expect(sawAbort).toBe(cancelling === callers)
        expect(store.writes).toHaveLength(cancelling === callers ? 0 : 1)
        expect(countOf(events, 'completed')).toBe(
          cancelling === callers ? 0 : 1
        )
        expect(await settles(() => coordinator.owners.size === 0)).toBe(true)
      } finally {
        await crossflight.close()
      }
    }
  )

  itProperty(
    'never starts a loader once every caller has gone',
    fc.record({
      value: fc.jsonValue(),
      callers: fc.integer({ min: 1, max: 5 }),
    }),
    async ({ value, callers }) => {
      const { store, coordinator, events, crossflight } = harness()

      try {
        const reasons = Array.from(
          { length: callers },
          (_, index) => new Error(`caller ${index} cancelled`)
        )
        const controllers = reasons.map(() => new AbortController())
        let runs = 0
        const loader = async (): Promise<{ run: number; value: unknown }> => {
          runs += 1
          return { run: runs, value }
        }

        const calls = controllers.map((controller) =>
          crossflight.wrap(KEY, loader, { signal: controller.signal })
        )

        // Handlers first: every cancellation below rejects its caller in this
        // task, and a rejection nobody is attached to yet is reported as an
        // unhandled one.
        const allSettled = Promise.allSettled(calls)

        // Every caller cancels in the task the flight was created in, so the
        // flight is aborted before it can reach its loader.
        controllers.forEach((controller, index) => {
          controller.abort(reasons[index])
        })
        const settled = await allSettled

        expect(runs).toBe(0)
        expect(store.writes).toEqual([])
        settled.forEach((outcome, index) => {
          expect(outcome.status).toBe('rejected')
          if (outcome.status === 'rejected') {
            expect(outcome.reason).toBe(reasons[index])
          }
        })
        expect(countOf(events, 'cancelled')).toBe(callers)
        // The flight still fails and reports it, once - nobody is left to throw
        // to, so the report is the only place the abort surfaces.
        expect(await settles(() => countOf(events, 'failed') === 1)).toBe(true)
        expect(await settles(() => coordinator.owners.size === 0)).toBe(true)

        // A caller arriving now must not inherit the abandoned flight's
        // cancellation: the record is winding down, so a fresh flight replaces
        // it and serves the value.
        const recovered = await crossflight.wrap(KEY, loader)
        expect(recovered).toEqual({ run: 1, value })
        expect(store.writes).toHaveLength(1)
        expect(await settles(() => coordinator.owners.size === 0)).toBe(true)
      } finally {
        await crossflight.close()
      }
    }
  )

  itProperty(
    'falls back to the loader instead of failing when the coordinator cannot lease',
    fc.record({
      value: fc.jsonValue(),
      callers: fc.integer({ min: 1, max: 6 }),
    }),
    async ({ value, callers }) => {
      const store = memoryStore()
      const unavailable = new Error('coordinator unavailable')
      const coordinator: Coordinator = {
        async acquire(): Promise<never> {
          throw unavailable
        },
        async waitForChange(): Promise<void> {},
        async close(): Promise<void> {},
      }
      const { events, crossflight } = harness({
        store,
        coordinator,
        failureMode: 'fail-open',
      })

      try {
        const gate = deferred()
        let runs = 0
        const loader = async (): Promise<{ run: number; value: unknown }> => {
          runs += 1
          await gate.promise
          return { run: runs, value }
        }

        const owner = crossflight.wrap(KEY, loader)
        await tick()
        const joiners = Array.from({ length: callers - 1 }, () =>
          crossflight.wrap(KEY, loader)
        )
        await tick()

        gate.resolve()
        const results = await Promise.all([owner, ...joiners])

        // The outage is reported once, not thrown, and the joiners share the one
        // fallback run instead of each starting a loader of its own.
        const failures = events.filter((event) => event.type === 'failed')
        expect(failures).toHaveLength(1)
        expect(failures[0]!.error).toBe(unavailable)
        expect(countOf(events, 'fallback')).toBe(1)
        expect(countOf(events, 'ownership_acquired')).toBe(0)
        expect(results).toHaveLength(callers)
        expect(results.every((result) => result.run === 1)).toBe(true)
        expect(runs).toBe(1)

        // Pinned as the current semantics of a fallback: it serves its callers
        // without publishing, so the next miss loads again rather than reading a
        // value that no owner wrote.
        expect(store.writes).toEqual([])
        const again = await crossflight.wrap(KEY, loader)
        expect(again.run).toBe(2)
        expect(store.writes).toEqual([])
      } finally {
        await crossflight.close()
      }
    }
  )

  itProperty(
    'serves a cached hit to every concurrent caller from one cache read',
    fc.record({
      value: fc.jsonValue(),
      callers: fc.integer({ min: 1, max: 6 }),
    }),
    async ({ value, callers }) => {
      const { store, coordinator, events, crossflight } = harness()
      store.seed(KEY, value)

      try {
        let runs = 0
        const loader = async (): Promise<{ run: number; value: unknown }> => {
          runs += 1
          return { run: runs, value }
        }

        // Callers that arrive together share one flight, and that flight is
        // settled by a single cache read: no loader, and not one read per caller.
        const results = await Promise.all(
          Array.from({ length: callers }, () => crossflight.wrap(KEY, loader))
        )

        expect(results.every((result) => Object.is(result, value))).toBe(true)
        expect(runs).toBe(0)
        expect(store.reads).toEqual([KEY])
        expect(store.writes).toEqual([])
        expect(countOf(events, 'hit')).toBe(1)
        expect(countOf(events, 'local_join')).toBe(callers - 1)
        expect(await settles(() => coordinator.owners.size === 0)).toBe(true)
      } finally {
        await crossflight.close()
      }
    }
  )
})
