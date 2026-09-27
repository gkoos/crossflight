---
'crossflight': minor
---

Cache loader results of `undefined` behind the new `cacheUndefined` option, which stores and unwraps a marked envelope so one cached miss is not treated as a miss by every later caller.
