---
'crossflight': patch
---

Acquire leases with a bare `SET NX` in the Redis Lua script instead of guarding it with a redundant `GET`. The script is atomic, so the extra read added a command execution without changing behavior.
