---
'crossflight': minor
---

Hand the shared flight's `AbortSignal` to the loader and stop waiting for it the moment the flight aborts, so a lost lease or `close()` rejects `wrap()` at once and abandons the lease instead of parking until the loader settles.
