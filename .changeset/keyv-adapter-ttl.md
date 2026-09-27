---
'crossflight': patch
---

Pass the cache TTL to Keyv as a number of milliseconds so cached values actually expire, and align the exported `KeyvLike` interface with Keyv's real `set()`/`get()` contract.
