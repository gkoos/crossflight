---
'crossflight': patch
---

Treat Redis change notifications as best effort: they are issued on the same connection as the lease mutation but never awaited, so a failed or stalled publish can neither fail an acquisition that already owns the lock and leave it orphaned until its TTL, nor delay or misreport a renewal or release that did succeed.
