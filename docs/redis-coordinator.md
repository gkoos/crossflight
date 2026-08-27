# Redis Coordinator

The Redis coordinator is the built-in distributed coordinator for Crossflight. It lets multiple processes agree on which one owns a cache miss and keeps waiting callers synchronized without requiring a custom coordination layer.

## Usage

```ts
import { Redis } from 'ioredis'
import { createCrossflight } from 'crossflight'
import { redisCoordinator } from 'crossflight/coordinators/redis'

const redis = new Redis()

const crossflight = createCrossflight({
  cache: existingCacheAdapter,
  coordinator: redisCoordinator(redis),
})

const value = await crossflight.wrap(
  'product:42',
  () => loadProductFromDatabase(42),
  { ttl: 30_000 },
)

await crossflight.close()
```

## What it coordinates

Each cache key is represented by two Redis primitives:

- a lease key, which records the current owner token and TTL
- a change channel, which notifies waiting callers when ownership changes

The default key layout is:

```text
crossflight:flight:<sha256(key)>
crossflight:change:<sha256(key)>
```

This keeps the coordination keys namespaced and avoids collisions between different applications or cache namespaces.

## Lease lifecycle

When a caller acquires a key, the coordinator tries to set the lease key atomically using a Redis Lua script. If the key is already present and still valid, acquisition returns `null` and the caller waits on the existing owner.

The lease includes:

- a unique owner token
- a TTL in milliseconds
- validation before renew, complete, or abandon operations

Every lease mutation is guarded by a Lua script that checks the current owner token before mutating the key. This prevents stale owners from deleting or renewing a lease for a newer owner.

### `renew()`

Extends the lease only if the current Redis value still matches the owner token.

### `complete()`

Deletes the lease key only if the current owner still matches the token, then publishes a change notification.

### `abandon()`

Same as `complete()` for a failed or canceled owner; it releases ownership and wakes waiters.

## Waiting for changes

`waitForChange()` subscribes to the per-key Redis Pub/Sub channel and resolves when the current owner completes, abandons, or renews the lease.

The implementation also supports a timeout and `AbortSignal`:

```ts
await crossflight.wrap('product:42', loadProduct, {
  signal: controller.signal,
  ttl: 30_000,
})
```

This is important because waiting callers must not block forever if Redis Pub/Sub is noisy, flaky, or missing a notification.

## Configuration

```ts
redisCoordinator(client, {
  namespace?: 'crossflight',
  hashKey?: (key: string) => string,
})
```

### `namespace`

Changes the prefix used for the lease and change keys.

```ts
const coordinator = redisCoordinator(redis, {
  namespace: 'my-app',
})
```

This produces keys such as:

```text
my-app:flight:<hash>
my-app:change:<hash>
```

### `hashKey`

Lets you customize the key hashing function. This is useful when you want a deterministic, application-specific namespace or when you want to avoid storing raw keys in Redis.

```ts
const coordinator = redisCoordinator(redis, {
  hashKey: key => `hashed:${key}`,
})
```

## Close semantics

`close()` cleans up listener state and unsubscribes from tracking channels. It also closes the main Redis client and the internal subscription client when they are still open.

It intentionally swallows harmless “connection is closed” errors during shutdown so a graceful teardown does not turn into a noisy failure.

## Correctness notes

The Redis coordinator is designed to avoid the usual distributed lock mistakes:

- stale owners cannot renew or delete a newer lease
- waiters time out instead of hanging forever
- closed or disconnected clients are rejected as coordinator errors
- ownership and notifications are kept separate so the lease can expire safely if a process crashes

## Related docs

- [Creating a coordinator](creating-a-coordinator.md)
- [Creating an adapter](creating-an-adapter.md)
- [README](../README.md)
