# Changelog

## 0.3.2

### Patch Changes

- bb8d3a9: Internal refactor with no change to the public API or to runtime behaviour: split the monolithic core module into focused pieces (`flight`, `caller`, `lease-keeper`, `envelope`, `async-utils`) with `core.ts` left as the composition root, and reorganised the test suite along the same boundaries.
- c746270: Internal test infrastructure with no change to the public API or to runtime behaviour: add generated test suites - fast-check property suites and fuzz transcripts - for the envelope contract, the Redis key layout, the coordinator contract and the flight state machine, run them under a deterministic suite-derived seed (`CROSSFLIGHT_TEST_SEED` explores another one), and wire `npm run typecheck` and a dedicated generated-suite job into CI.

## 0.3.1

### Patch Changes

- e63d05a: Accept the value cache-manager and Cacheable resolve from `set()`: the adapters ignore it instead of demanding `Promise<void>`, so a real typed instance satisfies the adapter interface without a cast.
- 457256a: Mark an instance closed before `close()` aborts its in-flight work, and reject every later `wrap()` with `CoordinationClosedError`, so closing cannot be undone by a cache hit or by a fail-open loader that runs once the coordinator has rejected its work.
- 61b84e4: Hold the whole-flight deadline over coordination and cleanup as well: acquisition, the distributed wait and renewal are raced with the flight signal, releasing a lease is handed to the coordinator instead of awaited, and a lease that arrives after the flight gave up waiting is abandoned in the background.
- c957645: Only treat a stored value as an encoded envelope when it has exactly the reserved shape - the marker and, optionally, a `value` - so an application object that carries `__crossflight_envelope__` next to fields of its own is returned as it was stored instead of being unwrapped into a `value`, or an `undefined`, that was never written under the key; a raw value of exactly the reserved shape stays indistinguishable from an envelope, so enabling `cacheUndefined` on an existing cache needs a migration or namespaced keys.
- 9a163e4: Keep renewing the lease while the cache write is in flight, so a write that outlives its lease can no longer let a replacement owner publish first and then be overwritten by the stale value.
- ba5b733: Treat Redis change notifications as best effort: they are issued on the same connection as the lease mutation but never awaited, so a failed or stalled publish can neither fail an acquisition that already owns the lock and leave it orphaned until its TTL, nor delay or misreport a renewal or release that did succeed.

## 0.3.0

### Minor Changes

- f303c56: Hand the shared flight's `AbortSignal` to the loader and stop waiting for it the moment the flight aborts, so a lost lease or `close()` rejects `wrap()` at once and abandons the lease instead of parking until the loader settles.
- cc8d876: Cache loader results of `undefined` behind the new `cacheUndefined` option, which stores every value in a reserved `{ "__crossflight_envelope__": 1, value }` envelope and unwraps it on read, so one cached miss is not treated as a miss by every later caller.
- df99d12: Give the coordination lease its own lifetime: `wrap()` now takes `leaseTtlMs` (defaulting to `defaultTtlMs`, floored at 50 ms), so the cache `ttl` no longer expires the lease before its first renewal or keeps a crashed owner's lease alive for as long as the value stays cached.
- 27b0ca5: Emit `cancelled`, `fallback` and `wait_exhausted` events, and report the distributed wait in `waitedMs` on `hit` and `completed`, so callers can see why work was skipped, joined, or run without ownership.
- 1ec10da: Support Redis Cluster: `redisCoordinator` accepts an ioredis `Cluster` as well as a `Redis` client, and keys are now tagged with a `{<hash>}` slot tag (`crossflight:{<hash>}:flight`) so every key derived from one logical key lives in the same cluster slot. The layout itself changed: processes running different Crossflight versions derive different keys and do not share leases, so upgrade without overlapping versions.
- 7cd4604: Add an opt-in whole-flight deadline - `defaultFlightDeadlineMs` on the factory or `flightDeadlineMs` per call - which aborts the shared flight when the budget passes, so joiners cannot each wait a fresh full timeout and a caller arriving afterwards is rejected instead of starting a new flight.

### Patch Changes

- df99d12: Settle waiters that are still parked when a coordinator is closed, instead of leaving them to time out on their own.
- 1a0a463: Trip the fail-open fallback only when a coordination call fails, not when `acquire()` reports the lease is held by another owner, so `failureMode: 'fail-open'` keeps coalescing calls under ordinary contention instead of running the loader in every process.
- 880d5e1: Pass the cache TTL to Keyv as a number of milliseconds so cached values actually expire, and align the exported `KeyvLike` interface with Keyv's real `set()`/`get()` contract.
- 4dfa9d9: Honour each caller's own `AbortSignal` and `timeoutMs` when it joins a shared in-process flight so a cancellation rejects only that caller, and abort the shared flight only when the last waiting caller cancels.
- 7ce1e6c: Drop a channel subscription once the last waiter for it is gone, including when a subscribe completes after its wait already settled, instead of tracking it until close().
- 0e0836c: Recover the Redis coordinator from a transient connection error instead of disabling it permanently: `close()`, an `end` event or a `close`/`end` client status stay terminal, an `error` is cleared once the client reports `ready`, and a broken subscription connection no longer blocks `acquire()`.
- 6fed389: Reject a waiter when its subscribe call fails with a closed-connection error, instead of leaving the wait pending forever and surfacing the translation as an unhandled rejection.
- 392c77f: Release the coordination lease when the ownership recheck finds a value another process cached, instead of holding ownership until it expires.

## 0.2.1

### Patch Changes

- 24a3f56: Emit exactly one `failed` event per error so `onEvent` consumers no longer observe duplicates when a rejection crosses an inner coordination catch before reaching the caller.
- 11f9ca9: Acquire leases with a bare `SET NX` in the Redis Lua script instead of guarding it with a redundant `GET`. The script is atomic, so the extra read added a command execution without changing behavior.
- f55063e: Unref the renewal and per-call timeout timers so a process that exits without calling `close()` is no longer held open waiting for them to fire.

## 0.2.0

### Minor Changes

- df11507: Add `renewal_failed` event and document the renewal-failure policy.

## 0.1.2

### Patch Changes

- 407a9ae: Improve distributed coordination and Redis lifecycle behavior for cross-process cache coalescing.

  - Renew distributed leases while owner loaders are still running to prevent premature lease expiry.
  - Reacquire ownership after wake-on-change plus cache miss, improving waiter recovery paths.
  - Add Redis command timeout support with explicit timeout errors for safer failure handling.
  - Clarify Redis client ownership: only internal subscription clients are closed by the coordinator.
  - Update docs and tests to reflect the new coordination semantics and operational expectations.

## 0.1.1

### Patch Changes

- Documented the Redis coordinator and distributed lease semantics.
- Expanded Redis integration coverage.
- Addressed CI and security hardening updates since the initial 0.1.0 release.

## 0.1.0 — 2026-08-27

Initial release.

- Generic `CacheAdapter` and `Coordinator` contracts - bring your own cache and coordinator backend
- Redis coordinator via `crossflight/coordinators/redis`
- Adapters for cache-manager, Keyv, and Cacheable
- Local in-process coalescing via shared in-flight Promise map
- Distributed coalescing with lease acquisition, renewal, and pub/sub wake-up
- Fail-open and fail-closed failure modes
- Per-call and global timeout support with `CoordinationTimeoutError`
- Typed error hierarchy: `CoordinationError`, `CoordinationClosedError`, `CoordinationTimeoutError`, `OwnershipLostError`
- Observability via `onEvent` hook with `onEventError` escape hatch
- Configurable `defaultTtlMs`, `maxRetryAttempts`, and `retryBackoff`
- Dual ESM/CJS build with tree-shakeable subpath exports
