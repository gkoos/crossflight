import * as fc from 'fast-check'
import { describe, expect, it } from 'vitest'

import { withCachedUndefined } from '../../src/envelope.js'
import type { CacheSetOptions } from '../../src/types.js'
import {
  jsonStore,
  memoryStore,
  normalizeThroughJson,
} from '../support/cache-store.js'
import { createPropertySuite } from '../support/seed.js'

/**
 * `cacheUndefined` rests on one discriminator: the exact shape of the reserved
 * envelope. Everything it promises - a value that looks like an envelope coming
 * back as a value, an application object carrying the reserved name untouched, a
 * stored `undefined` surviving a round trip - is a consequence of that
 * discriminator, so the properties below generate values that attack it.
 */
const itProperty = createPropertySuite('envelope', { runs: 150 })

const MARKER = '__crossflight_envelope__'

const jsonValue = fc.jsonValue()

/** Widens an arbitrary so differently-shaped ones can be mixed in one `oneof`. */
const aside = <Ts>(arbitrary: fc.Arbitrary<Ts>): fc.Arbitrary<unknown> =>
  arbitrary as fc.Arbitrary<unknown>

/**
 * `oneof` over widened arbitraries: each branch narrows to the same union, which
 * the caller names because fast-check cannot infer it from unlike branches.
 *
 * The rest parameter is a tuple of at least two branches: fast-check's own
 * signature accepts any array, so the tuple is what keeps a single branch from
 * being written as a `oneof`, which is only a longer spelling of that branch.
 */
const oneOf = <Ts>(
  ...arbitraries: [
    fc.Arbitrary<unknown>,
    fc.Arbitrary<unknown>,
    ...Array<fc.Arbitrary<unknown>>,
  ]
): fc.Arbitrary<Ts> => fc.oneof(...arbitraries) as fc.Arbitrary<Ts>

/** The exact envelopes Crossflight writes, plus values that merely resemble one. */
const reservedShapes = oneOf(
  aside(fc.constant({ [MARKER]: 1 })),
  aside(fc.record({ [MARKER]: fc.constant(1), value: jsonValue })),
  aside(
    fc.record({ [MARKER]: fc.constant(1), value: jsonValue, extra: jsonValue })
  )
)

/** Anything a loader may resolve, including the shapes Crossflight writes itself. */
const roundTripValue: fc.Arbitrary<unknown> = oneOf(
  aside(fc.constant(undefined)),
  aside(jsonValue),
  aside(reservedShapes)
)

/**
 * Objects that carry the marker without being exactly an envelope. Reading one
 * must hand back the object itself: unwrapping it would invent a `value` that
 * was never written under the key.
 */
const lookAlike: fc.Arbitrary<unknown> = oneOf(
  // The marker beside a field of its own.
  aside(
    fc.record({ [MARKER]: fc.constant(1), value: jsonValue, extra: jsonValue })
  ),
  // A near miss on the marker value.
  aside(
    fc.record({
      [MARKER]: fc.oneof(
        fc.constant(0),
        fc.constant('1'),
        fc.constant(2),
        fc.constant(null),
        fc.constant(true)
      ),
      value: jsonValue,
    })
  ),
  // The marker next to a field that is not `value`.
  aside(fc.record({ [MARKER]: fc.constant(1), other: jsonValue })),
  // The marker inherited rather than owned: only own keys count.
  aside(
    fc
      .record({ value: jsonValue })
      .map((value) => Object.assign(Object.create({ [MARKER]: 1 }), value))
  )
)

const cacheOptions: fc.Arbitrary<CacheSetOptions | undefined> = oneOf(
  aside(fc.constant(undefined)),
  aside(fc.record({ ttl: fc.integer({ min: -1, max: 1_000_000_000 }) }))
)

const envelopeOf = (write: unknown): Record<string, unknown> =>
  write as Record<string, unknown>

describe('cached undefined envelope', () => {
  itProperty(
    'returns a value written through the envelope unchanged',
    roundTripValue,
    async (value) => {
      const store = memoryStore()
      const cache = withCachedUndefined(store.adapter)

      await cache.set('k', value)
      const lookup = await cache.get<unknown>('k')

      expect(lookup.hit).toBe(true)
      if (lookup.hit) {
        expect(lookup.value).toEqual(value)
      }
    }
  )

  itProperty(
    'carries a value through a JSON store as the store would',
    roundTripValue,
    async (value) => {
      const store = jsonStore()
      const cache = withCachedUndefined(store.adapter)

      await cache.set('k', value)
      const lookup = await cache.get<unknown>('k')

      expect(lookup.hit).toBe(true)
      if (lookup.hit) {
        expect(lookup.value).toEqual(normalizeThroughJson(value))
      }
    }
  )

  itProperty(
    'writes one reserved envelope around the value and nothing else',
    fc.tuple(roundTripValue, cacheOptions),
    async ([value, options]) => {
      const store = memoryStore()
      const cache = withCachedUndefined(store.adapter)

      await cache.set('k', value, options)

      expect(store.writes).toHaveLength(1)
      const write = store.writes[0]!
      expect(write.key).toBe('k')
      expect(write.options).toEqual(options)

      const envelope = envelopeOf(write.value)
      expect(envelope[MARKER]).toBe(1)
      expect(Object.keys(envelope).sort()).toEqual(
        value === undefined ? [MARKER] : [MARKER, 'value']
      )

      if (value !== undefined) {
        // The value is put in the envelope, never copied or rewritten.
        expect(envelope.value).toBe(value)
      }
    }
  )

  itProperty(
    'handing a value over to the store does not change the caller copy',
    fc.dictionary(fc.constantFrom('a', 'b', 'value', MARKER), jsonValue, {
      maxKeys: 4,
    }),
    async (value) => {
      const store = memoryStore()
      const cache = withCachedUndefined(store.adapter)
      const snapshot = structuredClone(value)

      await cache.set('k', value)

      expect(value).toEqual(snapshot)
      // The original object is untouched: it is not the object that was stored.
      const write = store.writes[0]!
      expect(write.value).not.toBe(value)
      expect(envelopeOf(write.value).value).toBe(value)
    }
  )

  itProperty(
    'keeps __proto__, constructor and prototype as ordinary data',
    fc.dictionary(
      fc.constantFrom('__proto__', 'constructor', 'prototype'),
      fc.record({ polluted: fc.boolean() }),
      { minKeys: 1, maxKeys: 3 }
    ),
    async (payload) => {
      const store = jsonStore()
      const cache = withCachedUndefined(store.adapter)
      const value = Object.fromEntries(Object.entries(payload))

      await cache.set('k', value)
      const lookup = await cache.get<unknown>('k')

      expect(Object.prototype).not.toHaveProperty('polluted')
      expect(lookup.hit).toBe(true)
      if (lookup.hit) {
        expect(lookup.value).toEqual(value)
      }
    }
  )

  itProperty(
    'returns anything that is not exactly the envelope as it was stored',
    lookAlike,
    async (stored) => {
      const store = memoryStore()
      const cache = withCachedUndefined(store.adapter)
      store.seed('k', stored)

      const lookup = await cache.get<unknown>('k')

      expect(lookup.hit).toBe(true)
      if (lookup.hit) {
        // The very same object: not a synthesized `value`, not an `undefined`.
        expect(lookup.value).toBe(stored)
      }
    }
  )
})

/**
 * The behaviour the module's doc comments and
 * `CrossflightOptions.cacheUndefined` describe in prose, pinned as cases: the
 * prose is the contract, and this is what a change to it would break.
 */
describe('the reserved shape as a discriminator', () => {
  it('does not unwrap a look-alike carrying the reserved name beside its own fields', async () => {
    const store = memoryStore()
    const cache = withCachedUndefined(store.adapter)
    const stored = { [MARKER]: 1, value: 'legacy', extra: true }
    store.seed('k', stored)

    const lookup = await cache.get<unknown>('k')

    expect(lookup.hit).toBe(true)
    if (lookup.hit) {
      expect(lookup.value).toBe(stored)
    }
  })

  it('only unwraps the exact reserved shape, so a foreign one is indistinguishable', async () => {
    // Documented on CrossflightOptions.cacheUndefined: shape is the only
    // discriminator there is. Pinned so tightening or loosening it is a
    // decision rather than an accident.
    const store = memoryStore()
    const cache = withCachedUndefined(store.adapter)
    store.seed('value', {
      [MARKER]: 1,
      value: 'written-by-an-unguarded-process',
    })
    store.seed('undefined', { [MARKER]: 1 })

    const unwrapped = await cache.get<unknown>('value')
    const withoutValue = await cache.get<unknown>('undefined')

    expect(unwrapped).toEqual({
      hit: true,
      value: 'written-by-an-unguarded-process',
    })
    // A hit, not a miss: the envelope says "this key is cached", value absent.
    expect(withoutValue.hit).toBe(true)
    if (withoutValue.hit) {
      expect(withoutValue.value).toBeUndefined()
    }
  })

  it('leaves an object alone when the marker is inherited rather than owned', async () => {
    const store = memoryStore()
    const cache = withCachedUndefined(store.adapter)
    const stored = Object.assign(Object.create({ [MARKER]: 1 }), {
      value: 'own-field',
    })
    store.seed('k', stored)

    const lookup = await cache.get<unknown>('k')

    expect(lookup.hit).toBe(true)
    if (lookup.hit) {
      expect(lookup.value).toBe(stored)
    }
  })

  it('reports a stored undefined as a miss without the option and a hit with it', async () => {
    const store = jsonStore()

    await store.adapter.set('raw', undefined)
    await expect(store.adapter.get('raw')).resolves.toEqual({ hit: false })

    const cache = withCachedUndefined(store.adapter)
    await cache.set('wrapped', undefined)

    const lookup = await cache.get<unknown>('wrapped')
    expect(lookup.hit).toBe(true)
    if (lookup.hit) {
      expect(lookup.value).toBeUndefined()
    }
  })

  it('returns a miss untouched and reads the store once', async () => {
    const store = memoryStore()
    const cache = withCachedUndefined(store.adapter)

    await expect(cache.get('missing')).resolves.toEqual({ hit: false })
    expect(store.reads).toEqual(['missing'])
    expect(store.writes).toEqual([])
  })

  it('passes through values an in-memory store can hold that JSON cannot', async () => {
    // A JSON store cannot carry NaN or -0. That is JSON, not Crossflight: with
    // a store that keeps references, the envelope is transparent to them.
    const store = memoryStore()
    const cache = withCachedUndefined(store.adapter)

    for (const value of [NaN, -0, false, 0, '']) {
      await cache.set('k', value)

      const lookup = await cache.get<unknown>('k')
      expect(lookup.hit).toBe(true)
      if (lookup.hit) {
        expect(Object.is(lookup.value, value)).toBe(true)
      }
    }
  })
})
