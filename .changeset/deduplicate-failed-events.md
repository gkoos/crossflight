---
'crossflight': patch
---

Emit exactly one `failed` event per error so `onEvent` consumers no longer observe duplicates when a rejection crosses an inner coordination catch before reaching the caller.
