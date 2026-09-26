---
'crossflight': patch
---

Honour each caller's own `AbortSignal` and `timeoutMs` when it joins a shared in-process flight so a cancellation rejects only that caller, and abort the shared flight only when the last waiting caller cancels.
