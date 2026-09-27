---
'crossflight': minor
---

Emit `cancelled`, `fallback` and `wait_exhausted` events, and report the distributed wait in `waitedMs` on `hit` and `completed`, so callers can see why work was skipped, joined, or run without ownership.
