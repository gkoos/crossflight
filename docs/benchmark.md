# Benchmarking Crossflight

This benchmark measures duplicate expensive work under a realistic fan-out shape.

The question is not "is one code path faster in the abstract". The question is:

- how much duplicate work appears when multiple independent instances request the same key at the same time?
- how much of that duplication is removed by local in-memory coalescing?
- how much is removed by distributed Redis-backed coalescing?

The benchmark compares three scenarios for the same workload:

1. no coalescing
2. local coalescing
3. distributed coalescing via Redis

## What the workload does

Each instance issues `requestsPerInstance` requests for the same key. Those requests are scheduled with a configured `concurrency` value.

Each request then performs a real expensive CPU-bound task in a worker thread. The task is a PBKDF2-based crypto workload, not a sleep. The value `loaderMs` is the target cost for that expensive work. In other words, `loaderMs` defines how long the expensive work should take to complete, so the benchmark models genuine duplicate compute rather than just waiting on timers.

The benchmark reports:

- `expensiveLoadExecutions`: how many times the expensive work actually ran
- `fullBatchElapsedMs`: total wall-clock time for the full batch
- `userCpuMs`: process CPU spent during that batch

The important metric is the number of expensive executions under the same fan-out. That tells us whether we avoided duplicate work, and then we can compare elapsed time to see when the distributed path actually pays off.

## Default run

The default benchmark is intentionally tuned to a larger fan-out where the distributed path wins on elapsed time:

```bash
npm run benchmark
```

This runs:

- Redis startup
- the benchmark with default parameters
- Redis shutdown afterward

The default is:

```bash
--instances=20 --requests-per-instance=20 --concurrency=8 --loader-ms=2000
```

This is a real distributed-win shape, not a toy case. With this configuration, the expected pattern is:

- no coalescing: about `instances * requestsPerInstance` expensive executions
- local coalescing: about `instances` expensive executions
- distributed coalescing: about `1` expensive execution

and the distributed path is typically faster than the local path in wall time because the redundant work avoided is large enough to outweigh Redis coordination overhead.

## Overriding parameters

Use the CLI directly when you want a different shape, or pass overrides through npm with the extra separator required by npm's argument parsing:

```bash
npm run benchmark -- -- --instances=10 --requests-per-instance=10 --concurrency=8 --loader-ms=3000
```


You don't need to run `docker:up` and `docker:down` manually.

## Parameter semantics

- `instances`: number of independent processes participating in the same wave
- `requestsPerInstance`: number of duplicate requests each instance issues for the same key
- `concurrency`: how many requests are in flight at once within each instance

## When distributed coalescing wins

Distributed coalescing starts to win once the duplicate fan-out is large enough that the Redis coordination cost is smaller than the cost of doing the expensive work redundantly across processes.

In practice, that means:

- several independent instances are hitting the same key in the same window,
- each expensive load is sufficiently expensive to amortize Redis round-trips,
- the total duplicate work is large enough that a single shared execution materially reduces wall-clock time.

This benchmark is designed to make that crossover visible.
