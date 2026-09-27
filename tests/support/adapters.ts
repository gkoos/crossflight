import { createCache } from 'cache-manager'
import { Cacheable } from 'cacheable'
import { Keyv } from 'keyv'

import { cacheableAdapter } from '../../src/adapters/cacheable.js'
import { cacheManagerAdapter } from '../../src/adapters/cache-manager.js'
import { keyvAdapter } from '../../src/adapters/keyv.js'
import type { CacheAdapter } from '../../src/types.js'
import { normalizeThroughJson } from './cache-store.js'

/**
 * One shipped adapter over the real library it wraps.
 *
 * What an adapter promises is a promise about that library - Keyv reports a
 * stored `undefined` as a miss, Cacheable keeps a value as it was handed over -
 * so a fake with the same method names would only ever confirm what the adapter
 * already assumes. The unit suite pins each adapter's call shape; a generated
 * suite needs the library's own behaviour under many values.
 */
export interface AdapterSubject {
  name: string
  /** A fresh adapter over a fresh backend, without any Crossflight wrapping. */
  create(): CacheAdapter
  /**
   * What the backend does to a value on the way through: Keyv, which both the
   * Keyv and the cache-manager adapters hold, serializes to JSON, while
   * Cacheable keeps the value it was given. A test compares what it reads back
   * through this, never against the raw value.
   */
  roundTrip(value: unknown): unknown
}

export const shippedAdapters: AdapterSubject[] = [
  {
    name: 'keyv',
    create: () => keyvAdapter(new Keyv()),
    roundTrip: normalizeThroughJson,
  },
  {
    name: 'cache-manager',
    create: () => cacheManagerAdapter(createCache({ stores: [new Keyv()] })),
    roundTrip: normalizeThroughJson,
  },
  {
    name: 'cacheable',
    create: () => cacheableAdapter(new Cacheable()),
    roundTrip: (value) => value,
  },
]
