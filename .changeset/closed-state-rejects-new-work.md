---
'crossflight': patch
---

Mark an instance closed before `close()` aborts its in-flight work, and reject every later `wrap()` with `CoordinationClosedError`, so closing cannot be undone by a cache hit or by a fail-open loader that runs once the coordinator has rejected its work.
