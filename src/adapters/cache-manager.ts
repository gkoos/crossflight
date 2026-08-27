import type { CacheAdapter, CacheLookup, CacheSetOptions } from '../types.js'

export interface CacheManagerLike {
  get<T>(key: string): Promise<T | undefined>
  set<T>(key: string, value: T, ttl?: number | CacheSetOptions): Promise<void>
}

export function cacheManagerAdapter(cache: CacheManagerLike): CacheAdapter {
  return {
    async get<T>(key: string): Promise<CacheLookup<T>> {
      const value = await cache.get<T>(key)
      if (value === undefined) {
        return { hit: false }
      }
      return { hit: true, value }
    },
    async set<T>(key: string, value: T, options?: CacheSetOptions): Promise<void> {
      const ttl = options?.ttl
      if (ttl === undefined) {
        await cache.set(key, value)
        return
      }

      await cache.set(key, value, ttl)
    },
  }
}
