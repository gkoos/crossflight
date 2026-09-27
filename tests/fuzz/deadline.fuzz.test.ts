import * as fc from 'fast-check'
import { describe, expect } from 'vitest'

import { CoordinationTimeoutError } from '../../src/errors.js'
import { createCrossflight } from '../../src/index.js'
import type {
  CacheAdapter,
  CacheLookup,
  CoordinationFailureMode,
  Coordinator,
  CrossflightEvent,
} from '../../src/types.js'
import { InMemoryCoordinator } from '../mocks/in-memory-coordinator.js'
import { createPropertySuite } from '../support/seed.js'

/**
 * The deadline, the retry budget and the arrivals, fuzzed together.
 *
 * A generated scenario mixes the three bounds a caller can be under - its own
 * `timeoutMs`, the flight's per-call `flightDeadlineMs` and the instance's
 * `defaultFlightDeadlineMs` - with the reasons a flight cannot make progress:
 * someone else holding the lease (so the retry loop runs), a cache read that
 * never answers, and a loader that ignores its abort signal. The invariants are
 * about attribution and termination rather than about single outcomes, because
 * with three interacting clocks there is usually more than one right answer:
 *
 * - every call settles: a deadline exists to bound a call, so a hang is a
 *   failure of the property and is reported as one;
 * - no second loader run for the key while the first one's signal is live: the
 *   stampede protection, which a deadline must not trade away;
 * - a rejection carries the reason of the caller it belongs to - a cancellation
 *   is never handed to a caller that did not cancel, and a timeout never
 *   reports a duration another caller asked for;
 * - a caller that arrives after the deadline of a flight nothing can settle is
 *   rejected rather than served by it;
 * - a key someone else holds serves nobody in `fail-closed`;
 * - the retry loop waits exactly the backoffs it was configured with, and only
 *   as many times as its attempt budget;
 * - a served value was published by its owner, or served by a fail-open
 *   fallback - never invented by a flight that ran no loader.
 */
const itProperty = createPropertySuite('deadline', { runs: 40 })

const KEY = 'deadline:key'

/** How long the harness waits for a call before it calls it a hang. */
const SETTLE_TIMEOUT_MS = 2000

/**
 * How far past a deadline a caller has to arrive for the chronology to be out of
 * doubt: a timer fires late, never early, so this only has to absorb the
 * scheduling noise after the deadline.
 */
const LATE_MARGIN_MS = 15

interface Caller {
  /** When this caller arrives, measured from the first one. */
  delayMs: number
  /** This caller's own timeout, if any. */
  timeoutMs: number | null
  /** When this caller cancels its own wait, if it does. */
  cancelAfterMs: number | null
}

interface Backoff {
  base: number
  cap: number
}

interface Scenario {
  /** The instance-wide deadline, which is also what a later flight would get. */
  defaultDeadlineMs: number | null
  /** The deadline of the call that starts the flight, overriding the default. */
  flightDeadlineMs: number | null
  /** Someone else holds the lease for the whole run, so every attempt contends. */
  contended: boolean
  /** A cache read that never answers: only a deadline can bound it. */
  stallRead: boolean
  maxRetryAttempts: number
  backoff: Backoff
  loader: 'park' | 'settle'
  failureMode: CoordinationFailureMode
  /** The first caller starts the flight, so it is the one that arrives at once. */
  callers: Caller[]
}

const backoffFor =
  (backoff: Backoff) =>
  (attempt: number): number =>
    Math.min(backoff.base * (attempt + 1), backoff.cap)

const backoffSchedule = (scenario: Scenario): number[] =>
  Array.from({ length: scenario.maxRetryAttempts }, (_, attempt) =>
    backoffFor(scenario.backoff)(attempt)
  )

const deadlineOf = (scenario: Scenario): number | null =>
  scenario.flightDeadlineMs ?? scenario.defaultDeadlineMs

/**
 * A generated scenario has to terminate. A parked loader bounds nothing by
 * itself, so where a *new* flight would have no deadline to be bounded by, every
 * caller is given a timeout of its own; and a stalled read is only ever bounded
 * by a deadline, so it only appears in a scenario that has one.
 */
const terminating = (scenario: Scenario): Scenario => {
  const neverSettles =
    scenario.loader === 'park' &&
    (scenario.defaultDeadlineMs === null || scenario.defaultDeadlineMs <= 0)

  const callers = scenario.callers.map((caller, index) => {
    // The first caller starts the flight, so it is the one that arrives at once.
    const atOnce = index === 0 ? { ...caller, delayMs: 0 } : caller

    if (
      !neverSettles ||
      atOnce.timeoutMs !== null ||
      atOnce.cancelAfterMs !== null
    ) {
      return atOnce
    }

    // Bounded by the caller instead of by the flight: here both the retry loop
    // and the loader may be endless.
    return { ...atOnce, timeoutMs: 25 }
  })

  return {
    ...scenario,
    callers,
    stallRead: deadlineOf(scenario) === null ? false : scenario.stallRead,
  }
}

const callerArbitrary: fc.Arbitrary<Caller> = fc.record({
  delayMs: fc.constantFrom(0, 1, 3, 8, 20, 45, 90),
  timeoutMs: fc.option(fc.constantFrom(5, 12, 25, 60), { nil: null }),
  cancelAfterMs: fc.option(fc.constantFrom(0, 2, 6, 20), { nil: null }),
})

const scenarioArbitrary: fc.Arbitrary<Scenario> = fc
  .record({
    defaultDeadlineMs: fc.option(fc.constantFrom(5, 12, 25, 60), { nil: null }),
    flightDeadlineMs: fc.option(fc.constantFrom(5, 12, 25, 60), { nil: null }),
    contended: fc.boolean(),
    stallRead: fc.boolean(),
    maxRetryAttempts: fc.integer({ min: 1, max: 3 }),
    backoff: fc.record({
      base: fc.integer({ min: 1, max: 8 }),
      cap: fc.integer({ min: 1, max: 30 }),
    }),
    loader: fc.constantFrom('park' as const, 'settle' as const),
    failureMode: fc.constantFrom('fail-closed' as const, 'fail-open' as const),
    callers: fc.array(callerArbitrary, { minLength: 1, maxLength: 3 }),
  })
  .map(terminating)

type Outcome =
  | { status: 'pending' }
  | { status: 'timeout' }
  | { status: 'fulfilled'; value: unknown }
  | { status: 'rejected'; reason: unknown }

interface CallerRecord extends Caller {
  index: number
  reason: Error | null
  cancelled: boolean
  outcome: Outcome
}

interface Report {
  /** Everything that must never happen, described so a failure names it. */
  violations: string[]
  callers: CallerRecord[]
  /** Loader runs for the key, which is what a stampede would multiply. */
  runs: number
  /** Every `waitForChange` timeout the flight asked for, in order. */
  waits: number[]
  events: CrossflightEvent[]
  /** Whether the lease held by someone else survived the run. */
  holderStillOwns: boolean
  /** Leases left behind once everything had settled. */
  leasesLeft: number
}

const parked = <T>(): Promise<T> => new Promise<T>(() => {})

const tick = (): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, 0))

const settles = async (
  predicate: () => boolean,
  timeoutMs = 1000
): Promise<boolean> => {
  const deadline = Date.now() + timeoutMs

  while (!predicate() && Date.now() < deadline) {
    await tick()
  }

  return predicate()
}

/** Races one call against the harness bound, so a hang is reported not waited on. */
const raceSettle = async (call: Promise<unknown>): Promise<Outcome> => {
  let timer: ReturnType<typeof setTimeout> | undefined

  try {
    return await Promise.race([
      call.then(
        (value): Outcome => ({ status: 'fulfilled', value }),
        (reason): Outcome => ({ status: 'rejected', reason })
      ),
      new Promise<Outcome>((resolve) => {
        timer = setTimeout(
          () => resolve({ status: 'timeout' }),
          SETTLE_TIMEOUT_MS
        )
      }),
    ])
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer)
    }
  }
}

/**
 * Runs one scenario against a fresh instance, a healthy coordinator and a cache
 * that holds nothing: what is under test here is the timeline of the calls, not
 * the contents of the cache, which the transcript suite covers. Nothing is
 * asserted here - the property reads the report.
 */
const runScenario = async (scenario: Scenario): Promise<Report> => {
  const violations: string[] = []
  const events: CrossflightEvent[] = []
  const waits: number[] = []
  const coordinator = new InMemoryCoordinator()

  // A lease nobody in the scenario will release: every attempt contends, which
  // is what puts the retry loop on the timeline.
  if (scenario.contended) {
    await coordinator.acquire(KEY, { ttlMs: 10_000 })
  }

  const holderToken = coordinator.owners.get(KEY)?.ownerToken ?? null

  const cache: CacheAdapter = {
    async get<T>(): Promise<CacheLookup<T>> {
      // A read that never answers is the await a deadline also has to bound.
      return scenario.stallRead
        ? await parked<CacheLookup<T>>()
        : { hit: false }
    },
    async set(): Promise<void> {},
  }

  const counted: Coordinator = {
    acquire: (key, options) => coordinator.acquire(key, options),
    async waitForChange(key, options) {
      waits.push(options?.timeoutMs ?? -1)
      await coordinator.waitForChange(key, options)
    },
    close: () => coordinator.close(),
  }

  const crossflight = createCrossflight({
    cache,
    coordinator: counted,
    defaultFlightDeadlineMs: scenario.defaultDeadlineMs ?? undefined,
    maxRetryAttempts: scenario.maxRetryAttempts,
    retryBackoff: backoffFor(scenario.backoff),
    failureMode: scenario.failureMode,
    onEvent: (event) => events.push(event),
  })

  const live = new Map<string, AbortSignal>()
  let runs = 0

  const loader = async (signal: AbortSignal): Promise<unknown> => {
    const running = live.get(KEY)

    // Overlapping a winding-down loader is allowed, and only because its own
    // signal is already aborted: while a loader's signal is live, no second
    // loader for the key may start. That is the stampede protection, and this is
    // where a deadline could break it.
    if (running !== undefined && !running.aborted) {
      violations.push('two live loaders for the key')
    }

    live.set(KEY, signal)
    runs += 1

    try {
      return scenario.loader === 'park'
        ? await parked<unknown>()
        : { run: runs }
    } finally {
      if (live.get(KEY) === signal) {
        live.delete(KEY)
      }
    }
  }

  const records: CallerRecord[] = scenario.callers.map((caller, index) => ({
    ...caller,
    index,
    reason: null,
    cancelled: false,
    outcome: { status: 'pending' },
  }))

  /** The leases in flight that nobody in this scenario is entitled to hold. */
  const strayLeases = (): number =>
    holderToken !== null && coordinator.isCurrentOwner(KEY, holderToken)
      ? coordinator.owners.size - 1
      : coordinator.owners.size

  try {
    await Promise.all(
      records.map(async (record) => {
        await new Promise((resolve) => setTimeout(resolve, record.delayMs))

        const controller =
          record.cancelAfterMs === null ? undefined : new AbortController()

        if (controller !== undefined) {
          record.reason = new Error(`caller ${record.index} cancelled`)

          setTimeout(() => {
            // Only a caller that is still waiting cancels: one that has already
            // been served must not report a cancellation.
            if (record.outcome.status === 'pending') {
              record.cancelled = true
              controller.abort(record.reason)
            }
          }, record.cancelAfterMs ?? 0)
        }

        const call = crossflight.wrap(KEY, loader, {
          signal: controller?.signal,
          timeoutMs: record.timeoutMs ?? undefined,
          // The call that starts the flight decides the budget its joiners
          // inherit, and that call is this one.
          ...(record.index === 0
            ? { flightDeadlineMs: scenario.flightDeadlineMs ?? undefined }
            : {}),
        })

        record.outcome = await raceSettle(call)
      })
    )

    // Cleanup that a settled flight starts in the background needs a turn to
    // land before the leases it holds can be counted.
    await settles(() => strayLeases() === 0)

    return {
      violations,
      callers: records,
      runs,
      waits,
      events,
      holderStillOwns:
        holderToken !== null && coordinator.isCurrentOwner(KEY, holderToken),
      leasesLeft: strayLeases(),
    }
  } finally {
    await crossflight.close()
  }
}

/** A scenario with everything spelled out, so a focused property states one rule. */
const scenarioFor = (overrides: Partial<Scenario>): Scenario =>
  terminating({
    defaultDeadlineMs: null,
    flightDeadlineMs: null,
    contended: false,
    stallRead: false,
    maxRetryAttempts: 1,
    backoff: { base: 1, cap: 1 },
    loader: 'settle',
    failureMode: 'fail-closed',
    callers: [{ delayMs: 0, timeoutMs: null, cancelAfterMs: null }],
    ...overrides,
  })

/**
 * Reads one report as the invariants above, and names what broke. Every check is
 * a claim the README makes about a call under a bound, so a failure is a
 * sentence about the contract rather than about a value.
 */
const violationsOf = (scenario: Scenario, report: Report): string[] => {
  const violations = [...report.violations]
  const deadline = deadlineOf(scenario)
  const schedule = backoffSchedule(scenario)
  const cancellations = new Set(
    report.callers
      .map((caller) => caller.reason)
      .filter((reason): reason is Error => reason !== null)
  )

  for (const caller of report.callers) {
    const outcome = caller.outcome

    if (outcome.status === 'timeout') {
      violations.push(`caller ${caller.index} never settled`)
      continue
    }

    if (outcome.status === 'rejected') {
      if (caller.cancelled) {
        if (outcome.reason !== caller.reason) {
          violations.push(
            `caller ${caller.index} cancelled and rejected with another reason`
          )
        }
      } else if (cancellations.has(outcome.reason as Error)) {
        violations.push(
          `caller ${caller.index} did not cancel but rejected with a cancellation`
        )
      }

      if (outcome.reason instanceof CoordinationTimeoutError) {
        // A caller's own timeout names its own `after <n>ms`; the flight's
        // deadline names none, so nothing here may be another caller's number.
        const named = /\bafter (\d+)ms\b/.exec(outcome.reason.message)?.[1]

        if (named !== undefined && Number(named) !== caller.timeoutMs) {
          violations.push(
            `caller ${caller.index} rejected with a timeout of ${named}ms, which is not its own`
          )
        }
      }
    }

    // A flight that nothing can settle is bounded by its deadline alone, so a
    // caller arriving after it has to be turned away rather than served - with
    // the flight's timeout, or with the cancellation it issued itself. A fresh
    // flight for the same key is a new call with a new budget, and this caller
    // has no deadline of its own, so a cancellation is where it ends.
    const turnedAway =
      outcome.status === 'rejected' &&
      (outcome.reason instanceof CoordinationTimeoutError || caller.cancelled)

    if (
      scenario.loader === 'park' &&
      deadline !== null &&
      caller.delayMs >= deadline + LATE_MARGIN_MS &&
      !turnedAway
    ) {
      violations.push(
        `caller ${caller.index} arrived past the deadline and was not turned away`
      )
    }
  }

  // Every wait the retry loop made was one of the configured backoffs: the
  // schedule is the caller's, not the library's.
  for (const wait of report.waits) {
    if (!schedule.includes(wait)) {
      violations.push(
        `the flight waited ${wait}ms, which is not one of its backoffs`
      )
    }
  }

  const singleFlight =
    scenario.callers.length === 1 &&
    scenario.callers[0]!.cancelAfterMs === null &&
    scenario.callers[0]!.timeoutMs === null

  if (
    scenario.contended &&
    scenario.failureMode === 'fail-closed' &&
    deadline === null &&
    singleFlight
  ) {
    // Nothing else can end this flight, so its budget is spent exactly.
    if (report.waits.join(',') !== schedule.join(',')) {
      violations.push(
        `the retry loop waited [${report.waits.join(',')}] instead of [${schedule.join(',')}]`
      )
    }

    const exhausted = report.events.find(
      (event) => event.type === 'wait_exhausted'
    )

    if (
      exhausted === undefined ||
      exhausted.type !== 'wait_exhausted' ||
      exhausted.attempts !== scenario.maxRetryAttempts
    ) {
      violations.push('the retry budget was not reported as exhausted')
    }
  }

  const served = report.callers.filter(
    (caller) => caller.outcome.status === 'fulfilled'
  )

  // A key someone else holds has no owner to publish a value, and fail-closed
  // does not invent one.
  if (
    scenario.contended &&
    scenario.failureMode === 'fail-closed' &&
    served.length > 0
  ) {
    violations.push('a caller was served from a key another owner holds')
  }

  // A served value came from a flight that published it, or from a fail-open
  // fallback - never from a flight that never ran.
  if (
    scenario.failureMode === 'fail-closed' &&
    served.length > 0 &&
    !report.events.some((event) => event.type === 'completed')
  ) {
    violations.push('a caller was served without a completed flight')
  }

  // A free key with a loader that settles serves every caller that is still
  // there: only that caller's own bound takes it away, and the bounds here are
  // wide enough that scheduling cannot decide the outcome.
  if (
    !scenario.contended &&
    !scenario.stallRead &&
    scenario.loader === 'settle' &&
    (deadline === null || deadline >= 25)
  ) {
    for (const caller of report.callers) {
      const ownBoundIsWide = caller.timeoutMs === null || caller.timeoutMs >= 25

      if (
        !caller.cancelled &&
        ownBoundIsWide &&
        caller.outcome.status !== 'fulfilled'
      ) {
        violations.push(
          `caller ${caller.index} was not served although nothing bounded it that tightly`
        )
      }
    }
  }

  if (report.leasesLeft > 0) {
    violations.push(
      `${report.leasesLeft} lease(s) left behind once every call had settled`
    )
  }

  if (scenario.contended && !report.holderStillOwns) {
    violations.push('the flight released a lease that was not its own')
  }

  return violations
}

describe('a generated deadline and retry transcript', () => {
  itProperty(
    'holds its invariants however the calls arrive and the clocks collide',
    scenarioArbitrary,
    async (scenario) => {
      const report = await runScenario(scenario)

      // Termination, attribution, stampede protection, the retry schedule and
      // the lease bookkeeping, in one report: the entries name what broke.
      expect(violationsOf(scenario, report)).toEqual([])
    }
  )

  itProperty(
    'turns a late caller away instead of serving it from the flight it missed',
    fc.record({
      deadlineMs: fc.constantFrom(5, 12, 25),
      lateMs: fc.constantFrom(20, 45, 90),
    }),
    async ({ deadlineMs, lateMs }) => {
      // A loader that never settles, so a flight nothing can settle is still
      // winding down when the late caller arrives: rejecting it is the only
      // answer that does not turn a deadline into a stampede.
      const scenario = scenarioFor({
        defaultDeadlineMs: deadlineMs,
        loader: 'park',
        callers: [
          { delayMs: 0, timeoutMs: null, cancelAfterMs: null },
          {
            delayMs: deadlineMs + lateMs,
            timeoutMs: null,
            cancelAfterMs: null,
          },
        ],
      })

      const report = await runScenario(scenario)
      const late = report.callers[1]!.outcome

      expect(violationsOf(scenario, report)).toEqual([])
      expect(report.runs).toBeGreaterThan(0)
      expect(late.status).toBe('rejected')

      if (late.status === 'rejected') {
        expect(late.reason).toBeInstanceOf(CoordinationTimeoutError)
      }
    },
    // Each case parks a real deadline and a real arrival, so this is a coarse
    // sweep of the timeline rather than a fine one.
    { runs: 12 }
  )

  itProperty(
    'spends the retry budget on a contended key and reports exactly that',
    fc.record({
      maxRetryAttempts: fc.integer({ min: 1, max: 3 }),
      backoff: fc.record({
        base: fc.integer({ min: 1, max: 5 }),
        cap: fc.integer({ min: 1, max: 20 }),
      }),
    }),
    async ({ maxRetryAttempts, backoff }) => {
      // No deadline and no caller timeout: nothing but the retry budget can end
      // this flight, so what it waits is a function of the configuration alone.
      const scenario = scenarioFor({
        contended: true,
        maxRetryAttempts,
        backoff,
      })

      const report = await runScenario(scenario)
      const outcome = report.callers[0]!.outcome

      expect(violationsOf(scenario, report)).toEqual([])
      expect(report.waits).toEqual(backoffSchedule(scenario))
      // Contention is not a fallback in fail-closed, so the loader never ran.
      expect(report.runs).toBe(0)
      expect(outcome.status).toBe('rejected')

      if (outcome.status === 'rejected') {
        expect(outcome.reason).toBeInstanceOf(CoordinationTimeoutError)
      }
    },
    { runs: 20 }
  )

  itProperty(
    'bounds a cache read that never answers, without reaching the loader',
    fc.constantFrom(5, 12, 25),
    async (deadlineMs) => {
      const scenario = scenarioFor({
        defaultDeadlineMs: deadlineMs,
        stallRead: true,
      })

      const report = await runScenario(scenario)
      const outcome = report.callers[0]!.outcome

      expect(violationsOf(scenario, report)).toEqual([])
      expect(outcome.status).toBe('rejected')

      if (outcome.status === 'rejected') {
        expect(outcome.reason).toBeInstanceOf(CoordinationTimeoutError)
      }

      // The read was abandoned before ownership was taken or anything loaded.
      expect(report.runs).toBe(0)
      expect(report.waits).toEqual([])
    },
    { runs: 12 }
  )
})
