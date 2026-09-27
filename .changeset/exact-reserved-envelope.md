---
'crossflight': patch
---

Only treat a stored value as an encoded envelope when it has exactly the reserved shape - the marker and, optionally, a `value` - so an application object that carries `__crossflight_envelope__` next to fields of its own is returned as it was stored instead of being unwrapped into a `value`, or an `undefined`, that was never written under the key; a raw value of exactly the reserved shape stays indistinguishable from an envelope, so enabling `cacheUndefined` on an existing cache needs a migration or namespaced keys.
