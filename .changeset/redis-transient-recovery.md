---
'crossflight': patch
---

Recover the Redis coordinator from a transient connection error instead of disabling it permanently: `close()`, an `end` event or a `close`/`end` client status stay terminal, an `error` is cleared once the client reports `ready`, and a broken subscription connection no longer blocks `acquire()`.
