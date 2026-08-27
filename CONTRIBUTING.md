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

Lint and format:

```sh
npm run lint
npm run format
```

## Making changes

Changes to `src/` should include tests in `tests/`. New public API surface requires corresponding updates in `README.md` and, if relevant, `docs/`. Coordinators and adapters should remain behind their respective subpath exports (`crossflight/coordinators/*`, `crossflight/adapters/*`).

## Submitting a pull request

Run `npm run lint` and `npm run test:unit` locally before opening a PR. Integration tests run in CI automatically.

Every user-facing change requires a changeset entry. After staging your changes:

```sh
npx changeset add
```

Select the appropriate semver bump and write a concise description. The changeset file should be committed alongside the code change.

## Versioning

This project uses [Changesets](https://github.com/changesets/changesets). When a changeset PR is merged to `main`, the version workflow creates a version bump PR automatically. Merging that PR triggers the publish workflow.


## Submitting a pull request

Before opening a PR, run `npm run lint` and `npm run test:unit` locally. Integration tests run in CI automatically.

Every user-facing change requires a changeset entry. After staging your changes:

```sh
npx changeset add
```

Select the appropriate semver bump and write a concise description. The changeset file should be committed alongside the code change.

## Versioning

This project uses [Changesets](https://github.com/changesets/changesets). When a changeset PR is merged to `main`, the version workflow creates a version bump PR automatically. Merging that PR triggers the publish workflow.
