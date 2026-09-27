import type { CacheAdapter, CacheLookup, CacheSetOptions } from '../types.js'

export interface KeyvLike {
  /** Stored value, or `undefined` when the key is missing or expired. */
  get(key: string): Promise<unknown | undefined>
  /** Keyv takes the TTL in milliseconds as a number and resolves `true` once stored. */
  set(key: string, value: unknown, ttl?: number): Promise<boolean>
  delete?(key: string): Promise<boolean>
}

export function keyvAdapter(cache: KeyvLike): CacheAdapter {
  return {
    async get<T>(key: string): Promise<CacheLookup<T>> {
      // Keyv is not generic per call, so the caller's type is asserted here.
      const value = (await cache.get(key)) as T | undefined
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
