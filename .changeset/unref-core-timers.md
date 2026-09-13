---
'crossflight': patch
---

Unref the renewal and per-call timeout timers so a process that exits without calling `close()` is no longer held open waiting for them to fire.
