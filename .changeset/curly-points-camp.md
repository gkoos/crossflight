---
'crossflight': patch
---

Improve distributed coordination and Redis lifecycle behavior for cross-process cache coalescing.

- Renew distributed leases while owner loaders are still running to prevent premature lease expiry.
- Reacquire ownership after wake-on-change plus cache miss, improving waiter recovery paths.
- Add Redis command timeout support with explicit timeout errors for safer failure handling.
- Clarify Redis client ownership: only internal subscription clients are closed by the coordinator.
- Update docs and tests to reflect the new coordination semantics and operational expectations.
