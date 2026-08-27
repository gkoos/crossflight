# Changelog

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
