# Changelog

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
