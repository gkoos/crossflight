---
'crossflight': patch
---

Trip the fail-open fallback only when a coordination call fails, not when `acquire()` reports the lease is held by another owner, so `failureMode: 'fail-open'` keeps coalescing calls under ordinary contention instead of running the loader in every process.
