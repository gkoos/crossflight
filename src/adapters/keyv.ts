import type { CacheAdapter, CacheLookup, CacheSetOptions } from '../types.js'

export interface KeyvLike {
  get<T>(key: string): Promise<T | undefined>
  set<T>(key: string, value: T, options?: CacheSetOptions): Promise<T | void>
  delete?(key: string): Promise<boolean>
}

export function keyvAdapter(cache: KeyvLike): CacheAdapter {
  return {
    async get<T>(key: string): Promise<CacheLookup<T>> {
      const value = await cache.get<T>(key)
      if (value === undefined) {
        return { hit: false }
      }
      return { hit: true, value }
    },
    async set<T>(key: string, value: T, options?: CacheSetOptions): Promise<void> {
      await cache.set(key, value, options)
    },
  }
}
