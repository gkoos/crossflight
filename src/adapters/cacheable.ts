import type { CacheAdapter, CacheLookup, CacheSetOptions } from '../types.js'

export interface CacheableLike {
  get<T>(key: string): Promise<T | undefined>
  /**
   * Cacheable resolves a boolean that says whether the value was stored, so its
   * `set` is not a `Promise<void>`. The value is ignored here: accept whatever
   * it resolves.
   */
  set<T>(key: string, value: T, ttl?: number | CacheSetOptions): Promise<unknown>
}

export function cacheableAdapter(cache: CacheableLike): CacheAdapter {
  return {
    async get<T>(key: string): Promise<CacheLookup<T>> {
      const value = await cache.get<T>(key)
      if (value === undefined) {
        return { hit: false }
      }
      return { hit: true, value }
    },
    async set<T>(
      key: string,
      value: T,
      options?: CacheSetOptions
    ): Promise<void> {
      const ttl = options?.ttl
      if (ttl === undefined) {
        await cache.set(key, value)
        return
      }

      await cache.set(key, value, ttl)
    },
  }
}
