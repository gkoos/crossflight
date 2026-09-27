---
'crossflight': patch
---

Treat Redis change notifications as best effort, so a failed publish keeps the lease an acquisition already owns instead of failing the call and leaving the lock orphaned until its TTL, and no longer reports a renewal or release that did succeed as failed.
