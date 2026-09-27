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
crossflight:{<sha256(key)>}:flight
crossflight:{<sha256(key)>}:change
```

The `{...}` part is a Redis hash tag: because the tag is the hash of the logical key, every key derived from one logical key hashes to the same cluster slot, while different keys still spread across all slots. The change channel carries the tag too, for consistency - a Pub/Sub channel is not a stored key and no slot routes it, cluster Pub/Sub reaches every node on its own.

The namespace keeps the coordination keys namespaced and avoids collisions between different applications or cache namespaces.

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
  commandTimeoutMs?: number,
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
my-app:{<hash>}:flight
my-app:{<hash>}:change
```

### `hashKey`

Lets you customize the key hashing function. This is useful when you want a deterministic, application-specific namespace or when you want to avoid storing raw keys in Redis.

```ts
const coordinator = redisCoordinator(redis, {
  hashKey: key => `hashed:${key}`,
})
```

### `commandTimeoutMs`

Adds a per-command timeout guard to Redis operations used by the coordinator. This includes lease acquire, renew, complete/abandon, and the `waitForChange()` subscribe path.

```ts
const coordinator = redisCoordinator(redis, {
  commandTimeoutMs: 500,
})
```

## Redis Cluster

The coordinator runs against a cluster client unchanged. Pass an `ioredis` `Cluster` where you would pass a `Redis`: it only issues single-key commands and Pub/Sub, and the client routes both on its own.

```ts
import { Cluster } from 'ioredis'
import { createCrossflight } from 'crossflight'
import { redisCoordinator } from 'crossflight/coordinators/redis'

const cluster = new Cluster([
  { host: 'redis-1', port: 6379 },
  { host: 'redis-2', port: 6379 },
  { host: 'redis-3', port: 6379 },
])

const crossflight = createCrossflight({
  cache: existingCacheAdapter,
  coordinator: redisCoordinator(cluster),
})
```

What matters on a cluster:

- the tag keeps every key derived from one logical key in a single slot, so a future multi-key command or Lua script stays valid on a cluster and cluster tooling sees one shard per key
- Pub/Sub needs no slot affinity: it is cluster-wide, so a waiter on any node sees the owner's change notification
- the coordinator duplicates the client for its Pub/Sub connection, so a cluster client has to support `duplicate()` - `ioredis` does - and the client you pass stays caller-owned
- `namespace` and `hashKey` behave exactly as on a single node: processes only have to agree on those options and that they run the same Crossflight version

`npm run test:integration:cluster` runs the shared coordinator contract against a live cluster (the same spec the single-node coordinator and the in-memory mocks pass), plus cluster-specific checks for the tagged layout and cross-node ownership and wake-ups.

## Close semantics

`close()` cleans up listener state and unsubscribes from tracking channels. It closes only the internally created subscription client.

The Redis client passed to `redisCoordinator()` is caller-owned and is not closed by the coordinator. This allows a shared client lifecycle across multiple subsystems.

It intentionally swallows harmless “connection is closed” errors during shutdown so a graceful teardown does not turn into a noisy failure.

## Correctness notes

The Redis coordinator is designed to avoid the usual distributed lock mistakes:

- stale owners cannot renew or delete a newer lease
- waiters time out instead of hanging forever
- closed or disconnected clients are rejected as coordinator errors
- a transient connection error, by contrast, is recoverable: the coordinator resumes as soon as the client reports `ready`, and a broken subscription connection only blocks waiting - `acquire()` keeps working while the command connection is healthy
- a failed subscription rejects that waiter with a coordinator error instead of leaving it pending
- a channel subscription lives only as long as a waiter for it: the last waiter to leave unsubscribes, and a subscribe that resolves after its wait settled is dropped rather than tracked
- ownership and notifications are kept separate so the lease can expire safely if a process crashes

## Related docs

- [Creating a coordinator](creating-a-coordinator.md)
- [Creating an adapter](creating-an-adapter.md)
- [README](../README.md)
