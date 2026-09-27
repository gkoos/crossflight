---
'crossflight': minor
---

Cache loader results of `undefined` behind the new `cacheUndefined` option, which stores every value in a reserved `{ "__crossflight_envelope__": 1, value }` envelope and unwraps it on read, so one cached miss is not treated as a miss by every later caller.
