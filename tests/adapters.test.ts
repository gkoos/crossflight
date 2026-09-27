import Keyv from 'keyv'
import { describe, expect, it, vi } from 'vitest'

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

  it('adapts a real Keyv instance and applies the requested TTL', async () => {
    const keyv = new Keyv<number>()
    const adapter = keyvAdapter(keyv)
    const setSpy = vi.spyOn(keyv, 'set')

    await adapter.set('gamma', 42, { ttl: 30 })
    expect(setSpy).toHaveBeenCalledWith('gamma', 42, 30)

    await expect(adapter.get<number>('gamma')).resolves.toEqual({
      hit: true,
      value: 42,
    })

    await new Promise(resolve => setTimeout(resolve, 90))
    await expect(adapter.get<number>('gamma')).resolves.toEqual({ hit: false })

    setSpy.mockRestore()
  })

  it('adapts a real Keyv instance — set without TTL', async () => {
    const keyv = new Keyv()
    const adapter = keyvAdapter(keyv)

    await adapter.set('delta', 'value')

    await expect(adapter.get<string>('delta')).resolves.toEqual({
      hit: true,
      value: 'value',
    })

    await new Promise(resolve => setTimeout(resolve, 40))
    await expect(adapter.get<string>('delta')).resolves.toEqual({
      hit: true,
      value: 'value',
    })
    await expect(adapter.get<string>('missing')).resolves.toEqual({ hit: false })
  })
})
