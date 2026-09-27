---
'crossflight': patch
---

Accept the value cache-manager and Cacheable resolve from `set()`: the adapters ignore it instead of demanding `Promise<void>`, so a real typed instance satisfies the adapter interface without a cast.
