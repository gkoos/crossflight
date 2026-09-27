---
'crossflight': patch
---

Internal refactor with no change to the public API or to runtime behaviour: split the monolithic core module into focused pieces (`flight`, `caller`, `lease-keeper`, `envelope`, `async-utils`) with `core.ts` left as the composition root, and reorganised the test suite along the same boundaries.
