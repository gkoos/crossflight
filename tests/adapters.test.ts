import { describe, expect, it } from 'vitest'

import {
  cacheManagerAdapter,
  cacheableAdapter,
  keyvAdapter,
} from '../src/adapters/index.js'

describe('cache adapters', () => {
  it('adapts a cache-manager style cache', async () => {
    const values = new Map<string, unknown>()
    const adapter = cacheManagerAdapter({
      async get<T>(key: string): Promise<T | undefined> {
        return values.get(key) as T | undefined
      },
      async set<T>(key: string, value: T, ttl?: number): Promise<void> {
        values.set(key, value)
        void ttl
      },
    })

    await adapter.set('alpha', { ok: true }, { ttl: 5000 })

    await expect(adapter.get<{ ok: boolean }>('alpha')).resolves.toEqual({
      hit: true,
      value: { ok: true },
    })
    await expect(adapter.get<number>('missing')).resolves.toEqual({ hit: false })
  })

  it('adapts a cache-manager style cache — set without TTL', async () => {
    const setCalls: Array<[string, unknown, unknown]> = []
    const adapter = cacheManagerAdapter({
      async get<T>(_key: string): Promise<T | undefined> { return undefined },
      async set<T>(key: string, value: T, ttl?: number): Promise<void> {
        setCalls.push([key, value, ttl])
      },
    })

    await adapter.set('no-ttl', 'value')
    expect(setCalls).toHaveLength(1)
    expect(setCalls[0][2]).toBeUndefined()
  })

  it('adapts a Cacheable style cache', async () => {
    const values = new Map<string, unknown>()
    const adapter = cacheableAdapter({
      async get<T>(key: string): Promise<T | undefined> {
        return values.get(key) as T | undefined
      },
      async set<T>(key: string, value: T, ttl?: number): Promise<void> {
        values.set(key, value)
        void ttl
      },
    })

    await adapter.set('beta', 'value')
    await expect(adapter.get<string>('beta')).resolves.toEqual({
      hit: true,
      value: 'value',
    })
    await expect(adapter.get<string>('missing')).resolves.toEqual({ hit: false })
  })

  it('adapts a Cacheable style cache — set without TTL', async () => {
    const setCalls: Array<[string, unknown, unknown]> = []
    const adapter = cacheableAdapter({
      async get<T>(_key: string): Promise<T | undefined> { return undefined },
      async set<T>(key: string, value: T, ttl?: number): Promise<void> {
        setCalls.push([key, value, ttl])
      },
    })

    await adapter.set('no-ttl', 'value')
    expect(setCalls).toHaveLength(1)
    expect(setCalls[0][2]).toBeUndefined()
  })

  it('adapts a Cacheable style cache — set with TTL', async () => {
    const setCalls: Array<[string, unknown, unknown]> = []
    const adapter = cacheableAdapter({
      async get<T>(_key: string): Promise<T | undefined> { return undefined },
      async set<T>(key: string, value: T, ttl?: number): Promise<void> {
        setCalls.push([key, value, ttl])
      },
    })

    await adapter.set('with-ttl', 'value', { ttl: 3000 })
    expect(setCalls[0][2]).toBe(3000)
  })

  it('adapts a Keyv cache instance', async () => {
    const values = new Map<string, unknown>()
    const adapter = keyvAdapter({
      async get<T>(key: string): Promise<T | undefined> {
        return values.get(key) as T | undefined
      },
      async set<T>(key: string, value: T, options?: { ttl?: number }): Promise<void> {
        values.set(key, value)
        void options
      },
      async delete(key: string): Promise<boolean> {
        return values.delete(key)
      },
    })

    await adapter.set('gamma', 42, { ttl: 1000 })
    await expect(adapter.get<number>('gamma')).resolves.toEqual({
      hit: true,
      value: 42,
    })
    await expect(adapter.get<number>('missing')).resolves.toEqual({ hit: false })
  })
})
