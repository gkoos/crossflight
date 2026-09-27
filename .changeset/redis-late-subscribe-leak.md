---
'crossflight': patch
---

Drop a channel subscription once the last waiter for it is gone, including when a subscribe completes after its wait already settled, instead of tracking it until close().
