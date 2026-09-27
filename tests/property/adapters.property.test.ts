import * as fc from 'fast-check'
import { describe, expect } from 'vitest'

import { withCachedUndefined } from '../../src/envelope.js'
import { shippedAdapters } from '../support/adapters.js'
import { createPropertySuite } from '../support/seed.js'

/**
 * The shipped adapters over the real libraries they wrap: Keyv, cache-manager
 * over a Keyv store, and Cacheable. Each one promises the same thing - a value
 * that goes in comes back out as the backend transports it, a key that was
 * never written is a miss, a stored `undefined` is a miss too, and a `ttl`
 * bounds exactly the entry it was written with - and each element of that
 * promise is a property of the *library*, which no fake stands in for here.
 *
 * The last property is the one that ties this suite to
 * `CrossflightOptions.cacheUndefined`: the adapters' convention of reporting a
 * stored `undefined` as a miss is what the option exists to work around, so it
 * is pinned through the same real backends.
 */
const itProperty = createPropertySuite('adapters', { runs: 60 })

const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms))

/**
 * A key pool that is small enough to collide: an adapter that served a value
 * written under another key has to be caught, not made unlikely.
 */
const key: fc.Arbitrary<string> = fc.oneof(
  fc.constantFrom('shared', 'shared:2', 'KEY'),
  fc.string({ minLength: 1, maxLength: 8 })
)

describe('the shipped cache adapters over their real backends', () => {
  itProperty(
    'reads back whatever it stored, as the backend transports it',
    fc.record({ key, value: fc.jsonValue() }),
    async ({ key, value }) => {
      for (const subject of shippedAdapters) {
        const adapter = subject.create()

        await adapter.set(key, value)

        expect(await adapter.get(key), subject.name).toEqual({
          hit: true,
          value: subject.roundTrip(value),
        })
      }
    }
  )

  itProperty(
    'keeps every generated key apart from the others',
    fc.array(fc.record({ key, value: fc.jsonValue() }), {
      minLength: 1,
      maxLength: 6,
    }),
    async (entries) => {
      for (const subject of shippedAdapters) {
        const adapter = subject.create()
        // A key written twice is one key: the value of the last write is the
        // one a read has to see, and nothing else may have moved with it.
        const stored = new Map<string, unknown>()

        for (const entry of entries) {
          await adapter.set(entry.key, entry.value)
          stored.set(entry.key, subject.roundTrip(entry.value))
        }

        for (const [storedKey, value] of stored) {
          expect(
            await adapter.get(storedKey),
            `${subject.name} ${storedKey}`
          ).toEqual({ hit: true, value })
        }
      }
    }
  )

  itProperty(
    'reports a key it never stored as a miss',
    key,
    async (missing) => {
      for (const subject of shippedAdapters) {
        expect(await subject.create().get(missing), subject.name).toEqual({
          hit: false,
        })
      }
    }
  )

  itProperty(
    'expires what a ttl bounds and keeps what none does',
    fc.record({ key, ttl: fc.constantFrom(20, 40) }),
    async ({ key, ttl }) => {
      const bounded = `${key}:bounded`
      const forever = `${key}:forever`

      for (const subject of shippedAdapters) {
        const adapter = subject.create()

        await adapter.set(bounded, 'value', { ttl })
        await adapter.set(forever, 'value')

        // Readable to the last millisecond the ttl allows.
        expect(await adapter.get(bounded), subject.name).toEqual({
          hit: true,
          value: 'value',
        })

        await sleep(ttl + 60)

        expect(await adapter.get(bounded), subject.name).toEqual({ hit: false })
        // The age is the same, so the ttl is the only reason it went away.
        expect(await adapter.get(forever), subject.name).toEqual({
          hit: true,
          value: 'value',
        })
      }
    },
    { runs: 12 }
  )

  itProperty(
    'reports a stored undefined as a miss with the raw adapter and a hit through the envelope',
    key,
    async (key) => {
      for (const subject of shippedAdapters) {
        const raw = subject.create()
        await raw.set(key, undefined)

        // The convention every shipped adapter shares, and the reason
        // `CrossflightOptions.cacheUndefined` exists: the value is stored, and
        // the read cannot tell it from a missing key.
        expect(await raw.get(key), subject.name).toEqual({ hit: false })

        const wrapped = withCachedUndefined(subject.create())
        await wrapped.set(key, undefined)

        const lookup = await wrapped.get<unknown>(key)
        expect(lookup.hit, subject.name).toBe(true)

        if (lookup.hit) {
          expect(lookup.value, subject.name).toBeUndefined()
        }
      }
    }
  )
})
