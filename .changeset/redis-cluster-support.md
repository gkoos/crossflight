---
'crossflight': minor
---

Support Redis Cluster: `redisCoordinator` accepts an ioredis `Cluster` as well as a `Redis` client, and keys are now tagged with a `{<hash>}` slot tag (`crossflight:{<hash>}:flight`) so every key derived from one logical key lives in the same cluster slot. The layout itself changed: processes running different Crossflight versions derive different keys and do not share leases, so upgrade without overlapping versions.
