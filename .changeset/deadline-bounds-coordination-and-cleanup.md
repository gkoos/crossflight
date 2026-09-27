---
'crossflight': patch
---

Hold the whole-flight deadline over coordination and cleanup as well: acquisition, the distributed wait and renewal are raced with the flight signal, releasing a lease is handed to the coordinator instead of awaited, and a lease that arrives after the flight gave up waiting is abandoned in the background.
