import * as fc from 'fast-check'
import { it } from 'vitest'

/**
 * The seed protocol for generated suites.
 *
 * Generated runs are deterministic by default, unlike a classic fuzzing setup:
 * Crossflight gates every pull request on Codecov patch coverage
 * (`codecov.yml` has `require_ci_to_pass: true` and an informational-false
 * patch target), so a run that only fails on an unlucky seed would block a
 * merge that has nothing to do with the finding. The seed is therefore derived
 * from the suite name - stable across runs and machines - and exploration is
 * opt-in through `CROSSFLIGHT_TEST_SEED`.
 */
export const TEST_SEED_ENV = 'CROSSFLIGHT_TEST_SEED'

const DEFAULT_RUNS = 100

/**
 * FNV-1a over the suite name. Any stable function would do; it only has to be
 * reproducible and to spread different suite names across different seeds.
 */
function stableSeed(suite: string): number {
  let hash = 0x811c9dc5

  for (let index = 0; index < suite.length; index += 1) {
    hash ^= suite.charCodeAt(index)
    hash = Math.imul(hash, 0x01000193)
  }

  return hash | 0
}

/** The suite's seed: `CROSSFLIGHT_TEST_SEED` when set, the stable default otherwise. */
export function resolveTestSeed(suite: string): number {
  const configured = process.env[TEST_SEED_ENV]?.trim()

  if (!configured) {
    return stableSeed(suite)
  }

  const seed = Number(configured)

  if (!Number.isInteger(seed)) {
    throw new Error(
      `${TEST_SEED_ENV} must be an integer, received "${configured}"`
    )
  }

  return seed
}

/** The command that reproduces a failing run, whatever platform it is read on. */
export function replayHint(suite: string, seed: number): string {
  return [
    `seed ${seed} explored "${suite}" - replay it with:`,
    `  PowerShell: $env:${TEST_SEED_ENV}='${seed}'; npm run test:property`,
    `  POSIX:      ${TEST_SEED_ENV}=${seed} npm run test:property`,
  ].join('\n')
}

export interface PropertySuiteOptions {
  /** Generated cases per property. Raise it for a deep run, not in CI. */
  runs?: number
}

export interface PropertyOptions {
  runs?: number
}

/**
 * Builds the `it` a generated suite registers its properties with. Every
 * property runs under the suite's seed, and a failure is re-thrown with the
 * replay command attached: fast-check already reports the counterexample and
 * the shrink path, which is what the reader needs second.
 */
export function createPropertySuite(
  suite: string,
  options: PropertySuiteOptions = {}
) {
  const seed = resolveTestSeed(suite)
  const defaultRuns = options.runs ?? DEFAULT_RUNS

  return function itProperty<Ts>(
    title: string,
    arbitrary: fc.Arbitrary<Ts>,
    predicate: (value: Ts) => void | Promise<void>,
    overrides: PropertyOptions = {}
  ): void {
    it(title, async () => {
      try {
        await fc.assert(
          // Always asynchronous: a predicate that returns a promise and one
          // that returns nothing take the same path through the runner.
          fc.asyncProperty(arbitrary, async (value) => {
            await predicate(value)
          }),
          {
            seed,
            numRuns: overrides.runs ?? defaultRuns,
            verbose: fc.VerbosityLevel.Verbose,
          }
        )
      } catch (error) {
        const details = error instanceof Error ? error.message : String(error)

        throw new Error(
          `${title}\n\n${details}\n\n${replayHint(suite, seed)}`,
          { cause: error }
        )
      }
    })
  }
}
