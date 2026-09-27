---
'crossflight': minor
---

Support Redis Cluster: `redisCoordinator` accepts an ioredis `Cluster` as well as a `Redis` client, and the per-key lease and change keys now carry a `{<hash>}` tag (`crossflight:{<hash>}:flight`) so both land in the same cluster slot.
