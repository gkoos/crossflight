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

/**
 * The depth protocol for generated suites.
 *
 * A suite tunes its case count to what one of its cases costs, and a deep run
 * scales all of them together instead of replacing them with one number: a
 * transcript case costs milliseconds and has room for hundreds, while a case
 * that waits for a real ttl has room for tens. Unset means every suite runs the
 * count it chose.
 */
export const TEST_RUNS_ENV = 'CROSSFLIGHT_TEST_RUNS'

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

/**
 * Every suite's own case count, scaled by `CROSSFLIGHT_TEST_RUNS` when it is
 * set. The multiplier is read once per suite, so a deep run deepens every
 * property of it by the same factor.
 */
export function resolveRunsMultiplier(): number {
  const configured = process.env[TEST_RUNS_ENV]?.trim()

  if (!configured) {
    return 1
  }

  const multiplier = Number(configured)

  if (!Number.isInteger(multiplier) || multiplier < 1) {
    throw new Error(
      `${TEST_RUNS_ENV} must be a positive integer, received "${configured}"`
    )
  }

  return multiplier
}

/**
 * The command that reproduces a failing run, whatever platform it is read on. A
 * deep run is only replayable with its depth as well as its seed, so the
 * multiplier is part of the hint whenever it is not the default.
 */
export function replayHint(
  suite: string,
  seed: number,
  runsMultiplier = 1
): string {
  const assignments: Array<[string, number]> = [[TEST_SEED_ENV, seed]]

  if (runsMultiplier !== 1) {
    assignments.push([TEST_RUNS_ENV, runsMultiplier])
  }

  const powershell = assignments
    .map(([name, value]) => `$env:${name}='${value}'`)
    .join('; ')
  const posix = assignments.map(([name, value]) => `${name}=${value}`).join(' ')
  const depth = runsMultiplier === 1 ? '' : ` at ${runsMultiplier}x depth`

  return [
    `seed ${seed} explored "${suite}"${depth} - replay it with:`,
    `  PowerShell: ${powershell}; npm run test:generated`,
    `  POSIX:      ${posix} npm run test:generated`,
  ].join('\n')
}

export interface PropertySuiteOptions {
  /** Generated cases per property. Raise it for a deep run, not in CI. */
  runs?: number
}

export interface PropertyOptions {
  /** Generated cases for this property alone; scaled like the suite's own. */
  runs?: number
}

/**
 * Builds the `it` a generated suite registers its properties with. Every
 * property runs under the suite's seed - and at the suite's depth, scaled by
 * `CROSSFLIGHT_TEST_RUNS` - and a failure is re-thrown with the replay command
 * attached: fast-check already reports the counterexample and the shrink path,
 * which is what the reader needs second.
 */
export function createPropertySuite(
  suite: string,
  options: PropertySuiteOptions = {}
) {
  const seed = resolveTestSeed(suite)
  const runsMultiplier = resolveRunsMultiplier()
  const defaultRuns = (options.runs ?? DEFAULT_RUNS) * runsMultiplier

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
            numRuns:
              overrides.runs === undefined
                ? defaultRuns
                : overrides.runs * runsMultiplier,
            verbose: fc.VerbosityLevel.Verbose,
          }
        )
      } catch (error) {
        const details = error instanceof Error ? error.message : String(error)

        throw new Error(
          `${title}\n\n${details}\n\n${replayHint(suite, seed, runsMultiplier)}`,
          { cause: error }
        )
      }
    })
  }
}
