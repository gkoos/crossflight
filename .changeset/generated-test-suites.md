---
'crossflight': patch
---

Internal test infrastructure with no change to the public API or to runtime behaviour: add generated test suites - fast-check property suites and fuzz transcripts - for the envelope contract, the Redis key layout, the coordinator contract and the flight state machine, run them under a deterministic suite-derived seed (`CROSSFLIGHT_TEST_SEED` explores another one), and wire `npm run typecheck` and a dedicated generated-suite job into CI.
