---
'crossflight': patch
---

Reject a waiter when its subscribe call fails with a closed-connection error, instead of leaving the wait pending forever and surfacing the translation as an unhandled rejection.
