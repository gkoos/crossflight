---
'crossflight': minor
---

Give the coordination lease its own lifetime: `wrap()` now takes `leaseTtlMs` (defaulting to `defaultTtlMs`, floored at 50 ms), so the cache `ttl` no longer expires the lease before its first renewal or keeps a crashed owner's lease alive for as long as the value stays cached.
