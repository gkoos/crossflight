# Contributing

## Setup

Node.js 18 or higher and Docker are required. Clone the repository, then install dependencies:

```sh
npm install
```

## Development

Build the TypeScript source:

```sh
npm run build
```

Type-check without emitting:

```sh
npm run typecheck
```

Run unit tests (no Docker needed):

```sh
npm run test:unit
```

Run unit tests in watch mode:

```sh
npm run test:watch
```

Run integration tests against a real Redis instance (starts and stops Docker automatically):

```sh
npm run test:integration
```

Run integration tests against a Redis Cluster (starts and stops Docker automatically):

```sh
npm run test:integration:cluster
```

Run the full test suite:

```sh
npm run test
```

Run tests with combined coverage report (starts and stops Docker automatically):

```sh
npm run coverage
```

Run the generated suites (property tests over generated values, and fuzz tests over generated transcripts):

```sh
npm run test:generated
```

Lint and format:

```sh
npm run lint
npm run format
```

## Making changes

Changes to `src/` should include tests in `tests/`. New public API surface requires corresponding updates in `README.md` and, if relevant, `docs/`. Coordinators and adapters should remain behind their respective subpath exports (`crossflight/coordinators/*`, `crossflight/adapters/*`).

## Generated suites

Generated suites live in `tests/property/` (properties over generated values) and `tests/fuzz/` (fuzz transcripts over generated command sequences). They are kept out of `npm run test:unit` so that stays the fast, example-based loop, and they run in CI both as their own job and inside the combined coverage run.

Runs are deterministic: the seed is derived from the suite name, so a green run stays green on every machine and a red one reproduces. `CROSSFLIGHT_TEST_SEED` explores a different seed, and `CROSSFLIGHT_TEST_RUNS` scales every suite's case count - each suite is tuned to what one of its cases costs, so a deep run multiplies that choice instead of replacing it. A property whose cases park real wall-clock time - a real ttl, a real wait, a real deadline - caps that depth with `{ maxRuns }`, because there a case costs seconds and a ten-times run would outlast the config's timeout; twice its own count is the usual cap. A failing run prints the seed, the depth and the command to replay it:

```sh
CROSSFLIGHT_TEST_SEED=123456789 CROSSFLIGHT_TEST_RUNS=10 npm run test:generated
```

`npm run test:generated:deep` is that deep run under the default seed, and `.github/workflows/nightly.yml` runs it every night with a fresh seed, together with the Redis integration suites - the only place where a real Redis is fuzzed.

Build a suite with `createPropertySuite` from `tests/support/seed.ts` so it registers under that protocol. Shared harnesses live in `tests/support/`: `cache-store.ts` records reads and writes and can seed the store, `adapters.ts` builds the shipped adapters over the real libraries they wrap, `fake-redis.ts` runs the Redis coordinator without a socket - it can script replies, park a command and drive the connection events - and `coordinator-model.ts` is the coordinator contract as a reference model, for differential runs.

A generated suite is only worth its runtime if it can fail, so a new property should be shown to fail against a deliberate mutation of the code it is about: comment the line out, watch the property fail, put it back. Anything that cannot be made to fail that way is describing the implementation to itself rather than the contract.

## Submitting a pull request

Run `npm run lint` and `npm run test:unit` locally before opening a PR. Integration tests run in CI automatically.

Every user-facing change requires a changeset entry. After staging your changes:

```sh
npx changeset add
```

Select the appropriate semver bump and write a concise description. The changeset file should be committed alongside the code change.

## Versioning

This project uses [Changesets](https://github.com/changesets/changesets). When a changeset PR is merged to `main`, the version workflow creates a version bump PR automatically. Merging that PR triggers the publish workflow.
