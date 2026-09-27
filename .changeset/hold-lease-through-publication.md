---
'crossflight': patch
---

Keep renewing the lease while the cache write is in flight, so a write that outlives its lease can no longer let a replacement owner publish first and then be overwritten by the stale value.
