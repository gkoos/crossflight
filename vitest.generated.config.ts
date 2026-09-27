import { defineConfig } from 'vitest/config'

/**
 * Generated suites: property tests over generated values and fuzz tests over
 * generated transcripts.
 *
 * They are kept out of `vitest.config.ts` so that `npm run test:unit` stays the
 * fast, example-based loop, and included in `vitest.coverage.config.ts` so the
 * combined coverage run executes them like any other suite.
 *
 * Runs are deterministic: the seed comes from the suite name, and
 * `CROSSFLIGHT_TEST_SEED` explores a different one. How many cases a suite runs
 * is its own choice - each one is tuned to what a case costs - scaled by
 * `CROSSFLIGHT_TEST_RUNS` for a deep run, except where a case parks real
 * wall-clock time and depth would buy seconds rather than cases: there the
 * property caps its own depth (`maxRuns`). See `tests/support/seed.ts`.
 */
export default defineConfig({
  test: {
    environment: 'node',
    globals: true,
    include: ['tests/property/**/*.test.ts', 'tests/fuzz/**/*.test.ts'],
    testTimeout: 30000,
  },
})
