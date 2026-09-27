import * as fc from 'fast-check'
import { describe, expect } from 'vitest'

import { createCrossflight } from '../../src/index.js'
import type { CacheAdapter, Crossflight } from '../../src/types.js'
import { InMemoryCoordinator } from '../mocks/in-memory-coordinator.js'
import { shippedAdapters } from '../support/adapters.js'
import {
  jsonStore,
  memoryStore,
  normalizeThroughJson,
  type StoreHarness,
} from '../support/cache-store.js'
import { createPropertySuite } from '../support/seed.js'

/**
 * `cacheUndefined` through a whole instance, over the backends that are shipped
 * and the backend conventions that exist: a store that keeps references, a
 * JSON store, and the three real adapters. The envelope suite checks the
 * discriminator on its own; the question here is the one a user asks - a loader
 * that resolves `undefined` runs once and is then served from the cache, and a
 * loader value of any shape comes back exactly as it was loaded.
 *
 * The option is also a migration: a value of the reserved shape that a process
 * with the option off left behind reads back as an envelope, and the properties
 * pin that as a decision rather than an accident.
 */
const itProperty = createPropertySuite('cache-undefined', { runs: 60 })

const MARKER = '__crossflight_envelope__'
const KEY = 'cached:undefined'

interface Opened {
  adapter: CacheAdapter
  /** Present when the backend records what it was asked to do. */
  store?: StoreHarness
}

interface Backend {
  name: string
  /** A fresh adapter over a fresh backend, without any Crossflight wrapping. */
  open(): Opened
  /** What the backend does to a value on the way through. */
  roundTrip(value: unknown): unknown
  /**
   * Whether a stored `undefined` reads back as a miss. That convention is the
   * whole reason the option exists, and a store that keeps references does not
   * have it.
   */
  reportsStoredUndefinedAsMiss: boolean
}

const backends: Backend[] = [
  {
    name: 'a store that keeps references',
    open: () => {
      const store = memoryStore()
      return { adapter: store.adapter, store }
    },
    roundTrip: (value) => value,
    reportsStoredUndefinedAsMiss: false,
  },
  {
    name: 'a store that serializes to JSON',
    open: () => {
      const store = jsonStore()
      return { adapter: store.adapter, store }
    },
    roundTrip: normalizeThroughJson,
    reportsStoredUndefinedAsMiss: true,
  },
  ...shippedAdapters.map((subject) => ({
    name: subject.name,
    open: (): Opened => ({ adapter: subject.create() }),
    roundTrip: subject.roundTrip,
    reportsStoredUndefinedAsMiss: true,
  })),
]

interface Session {
  crossflight: Crossflight
  opened: Opened
}

const withInstance = async <T>(
  backend: Backend,
  options: { cacheUndefined: boolean },
  run: (session: Session) => Promise<T>
): Promise<T> => {
  const opened = backend.open()
  const crossflight = createCrossflight({
    cache: opened.adapter,
    coordinator: new InMemoryCoordinator(),
    cacheUndefined: options.cacheUndefined,
    // Nothing here contends, so the retry budget only has to be present: a
    // contention that needed retrying at all would be another suite's finding.
    maxRetryAttempts: 1,
    retryBackoff: () => 1,
  })

  try {
    return await run({ crossflight, opened })
  } finally {
    await crossflight.close()
  }
}

/** Objects that carry the reserved name without being exactly an envelope. */
const lookAlike: fc.Arbitrary<unknown> = fc.oneof(
  fc.constant({ [MARKER]: 1 }),
  fc.record({ [MARKER]: fc.constant(1), value: fc.jsonValue() }),
  fc.record({
    [MARKER]: fc.constant(1),
    value: fc.jsonValue(),
    extra: fc.constant(true),
  }),
  fc.record({
    [MARKER]: fc.oneof(fc.constant(0), fc.constant('1')),
    value: fc.jsonValue(),
  })
)

/** Anything a loader may resolve, including the shapes Crossflight writes itself. */
const loaderValue: fc.Arbitrary<unknown> = fc.oneof(
  fc.constant(undefined),
  fc.jsonValue(),
  lookAlike
)

describe('cacheUndefined through a whole instance', () => {
  itProperty(
    'runs the loader once and serves the undefined it resolved to every caller',
    fc.integer({ min: 1, max: 4 }),
    async (callers) => {
      for (const backend of backends) {
        let runs = 0
        const values: unknown[] = []

        await withInstance(
          backend,
          { cacheUndefined: true },
          async ({ crossflight, opened }) => {
            const loader = async (): Promise<undefined> => {
              runs += 1
              return undefined
            }

            for (let caller = 0; caller < callers; caller += 1) {
              values.push(await crossflight.wrap(KEY, loader))
            }

            if (opened.store) {
              // One write, and it is the reserved envelope with no `value`: the
              // only thing that can carry an `undefined` through these stores.
              expect(opened.store.writes, backend.name).toHaveLength(1)
              expect(opened.store.writes[0]!.value, backend.name).toEqual({
                [MARKER]: 1,
              })
            }
          }
        )

        expect(runs, backend.name).toBe(1)
        expect(values, backend.name).toHaveLength(callers)
        expect(
          values.every((value) => value === undefined),
          backend.name
        ).toBe(true)
      }
    }
  )

  itProperty(
    'is what makes the undefined cacheable where the backend reports it as a miss',
    fc.integer({ min: 1, max: 4 }),
    async (callers) => {
      for (const backend of backends) {
        let runs = 0

        await withInstance(
          backend,
          { cacheUndefined: false },
          async ({ crossflight }) => {
            for (let caller = 0; caller < callers; caller += 1) {
              await crossflight.wrap(KEY, () => {
                runs += 1
                return undefined
              })
            }
          }
        )

        // Without the option the loader runs for every caller exactly where the
        // backend cannot carry a stored `undefined` back as a hit, and nowhere
        // else: the option is a workaround, not a rewrite of cache semantics.
        expect(runs, backend.name).toBe(
          backend.reportsStoredUndefinedAsMiss ? callers : 1
        )
      }
    }
  )

  itProperty(
    'returns a loader value of any shape exactly as the backend transports it',
    loaderValue,
    async (value) => {
      for (const backend of backends) {
        await withInstance(
          backend,
          { cacheUndefined: true },
          async ({ crossflight }) => {
            const loaded = await crossflight.wrap(KEY, () => value)
            expect(loaded, backend.name).toEqual(backend.roundTrip(value))

            // Whatever came back has to have been served from what was stored:
            // a second loader run would mean the value did not survive.
            const served = await crossflight.wrap(KEY, () => {
              throw new Error('the loader ran again')
            })

            expect(served, backend.name).toEqual(backend.roundTrip(value))
          }
        )
      }
    }
  )

  itProperty(
    'unwraps a reserved shape a process without the option left behind',
    fc.record({ value: fc.jsonValue() }),
    async ({ value }) => {
      for (const backend of backends) {
        let runs = 0

        await withInstance(
          backend,
          { cacheUndefined: true },
          async ({ crossflight, opened }) => {
            // The raw shape, written the way an unguarded process or an older
            // version would have written it.
            await opened.adapter.set(KEY, { [MARKER]: 1, value })

            const served = await crossflight.wrap(KEY, () => {
              runs += 1
              return 'loaded'
            })

            expect(served, backend.name).toEqual(backend.roundTrip(value))
          }
        )

        expect(runs, backend.name).toBe(0)
      }
    }
  )

  itProperty(
    'writes the loader value itself when the option is off',
    fc.jsonValue(),
    async (value) => {
      for (const backend of backends) {
        await withInstance(
          backend,
          { cacheUndefined: false },
          async ({ crossflight, opened }) => {
            await crossflight.wrap(KEY, () => value)

            if (opened.store) {
              // Opt-in means transparent when off: no marker, no wrapper - the
              // value the loader produced is what the cache was handed.
              expect(opened.store.writes, backend.name).toHaveLength(1)
              expect(opened.store.writes[0]!.value, backend.name).toEqual(value)
            }
          }
        )
      }
    }
  )
})
