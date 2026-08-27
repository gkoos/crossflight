# Creating a Cache Adapter

A cache adapter bridges Crossflight to your existing cache backend. It handles reading and writing cached values.

## The CacheAdapter Interface

```ts
export interface CacheAdapter {
  get<T>(key: string): Promise<CacheLookup<T>>
  set<T>(key: string, value: T, options?: CacheSetOptions): Promise<void>
}

export type CacheLookup<T> = { hit: true; value: T } | { hit: false }

export interface CacheSetOptions {
  ttl?: number
}
```

## Key Points

- **`get()` must distinguish hit from miss** — return `{ hit: false }` for missing keys, not `undefined`. This matters because `undefined` can be a valid cached value.
- **`set()` should respect TTL if provided** — the `ttl` is in milliseconds.
- **Both methods must be async** — even if your cache is synchronous, wrap the result in a Promise.
- **Errors should propagate** — Crossflight expects to see failures from your cache backend.

## Example: MongoDB Adapter

```ts
import type { CacheAdapter, CacheLookup, CacheSetOptions } from 'crossflight'

export interface MongoDBAdapterOptions {
  collection: any // MongoDB collection
  defaultTtlMs?: number
}

export function mongodbAdapter(options: MongoDBAdapterOptions): CacheAdapter {
  const { collection, defaultTtlMs = 3600000 } = options

  return {
    async get<T>(key: string): Promise<CacheLookup<T>> {
      const doc = await collection.findOne({ _id: key })
      
      if (!doc) {
        return { hit: false }
      }

      // Check if expired (MongoDB TTL index would auto-delete, but explicit check doesn't hurt)
      if (doc.expiresAt && Date.now() > doc.expiresAt) {
        return { hit: false }
      }

      return { hit: true, value: doc.value as T }
    },

    async set<T>(key: string, value: T, options?: CacheSetOptions): Promise<void> {
      const ttlMs = options?.ttl ?? defaultTtlMs
      
      await collection.updateOne(
        { _id: key },
        {
          $set: {
            value,
            expiresAt: Date.now() + ttlMs,
          },
        },
        { upsert: true }
      )
    },
  }
}
```

## Example: Redis Adapter

```ts
import type { CacheAdapter, CacheLookup, CacheSetOptions } from 'crossflight'
import { Redis } from 'ioredis'

export function redisAdapter(redis: Redis): CacheAdapter {
  return {
    async get<T>(key: string): Promise<CacheLookup<T>> {
      const value = await redis.get(key)
      
      if (value === null) {
        return { hit: false }
      }

      return { hit: true, value: JSON.parse(value) as T }
    },

    async set<T>(key: string, value: T, options?: CacheSetOptions): Promise<void> {
      const serialized = JSON.stringify(value)
      
      if (options?.ttl) {
        await redis.psetex(key, options.ttl, serialized)
      } else {
        await redis.set(key, serialized)
      }
    },
  }
}
```

## Error Handling

Errors from your cache backend should propagate:

```ts
export function customAdapter(backend: CustomCache): CacheAdapter {
  return {
    async get<T>(key: string): Promise<CacheLookup<T>> {
      try {
        const value = await backend.get(key)
        if (value === undefined) {
          return { hit: false }
        }
        return { hit: true, value }
      } catch (error) {
        // Let the error propagate. Crossflight will handle it
        // based on the failureMode setting.
        throw error
      }
    },

    async set<T>(key: string, value: T, options?: CacheSetOptions): Promise<void> {
      // This error will propagate too
      await backend.set(key, value, { ttl: options?.ttl })
    },
  }
}
```

## Using Your Adapter

```ts
import { createCrossflight } from 'crossflight'
import { mongodbAdapter } from './adapters/mongodb-adapter.js'

const crossflight = createCrossflight({
  cache: mongodbAdapter({ collection: myCollection }),
  coordinator: redisCoordinator(redis),
})

const value = await crossflight.wrap('key', () => expensiveLoad())
```

## Publishing Your Adapter

If you want to share your adapter as a package:

1. Create a new npm package (e.g., `@myorg/crossflight-mongodb-adapter`)
2. Add `crossflight` as a peer dependency
3. Export your adapter factory function
4. Document the setup and options

Users can then install and use it:

```ts
import { mongodbAdapter } from '@myorg/crossflight-mongodb-adapter'
```
