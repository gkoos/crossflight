import * as fc from 'fast-check'
import { describe, expect } from 'vitest'

import { createCrossflight } from '../../src/index.js'
import type {
  CacheAdapter,
  CacheLookup,
  CacheSetOptions,
  CoordinationFailureMode,
  Coordinator,
  Lease,
} from '../../src/types.js'
import { InMemoryCoordinator } from '../mocks/in-memory-coordinator.js'
import { memoryStore, type StoreHarness } from '../support/cache-store.js'
import { createPropertySuite } from '../support/seed.js'

/**
 * A fuzz transcript is a generated sequence of steps that share one Crossflight
 * instance, and one backend. Each step starts a group of calls for one key, may
 * cancel some of them while the loader is parked, may start further callers
 * while the group winds down, and then releases the loader. On top of that the
 * coordinator and the store are given a *fault plan*: the nth call of a given
 * kind fails, which is the state machine's other half - what happens when the
 * lease cannot be taken, renewed, or released, or when the cache itself fails
 * mid-flight.
 *
 * The assertions are invariants rather than expected outcomes, because with
 * this many interacting failures there is no single right answer to compare
 * against - but there are always things that must never happen:
 *
 * - no two loaders for one key are ever live at once while the first one's
 *   signal is still live: that is the stampede protection itself, and a
 *   cancelled flight's loader may only overlap a new one, never a live one;
 * - a caller only ever receives a value that a loader produced for that key -
 *   never another key's, never a value nobody wrote;
 * - a cancellation rejects that caller with its own reason, and no other
 *   caller's promise rejects with it;
 * - every call settles (a hung flight is a failure too), and no lease is left
 *   behind unless the release the plan called for actually failed; a fault
 *   index the run never reached excuses nothing.
 */
const itProperty = createPropertySuite('transcript', { runs: 30 })

const SETTLE_TIMEOUT_MS = 3000

interface Step {
  key: string
  value: unknown
  callers: number
  loader: 'resolve' | 'reject'
  /** Per caller index: cancels while the loader is parked. */
  cancel: boolean[]
  /** Callers that arrive while the group winds down. */
  revive: number
}

interface FaultPlan {
  acquire: number | null
  waitForChange: number | null
  /** Renewal is what keeps ownership, so losing it gets its own injected mode. */
  renew: number | null
  complete: number | null
  abandon: number | null
  get: number | null
  set: number | null
}

interface Scenario {
  transcript: Step[]
  faults: FaultPlan
  failureMode: CoordinationFailureMode
}

const stepArbitrary: fc.Arbitrary<Step> = fc.record({
  // A small key space, so consecutive steps actually meet in the cache.
  key: fc.constantFrom('alpha', 'beta'),
  value: fc.jsonValue(),
  callers: fc.integer({ min: 1, max: 3 }),
  loader: fc.constantFrom('resolve', 'reject'),
  cancel: fc.array(fc.boolean(), { minLength: 3, maxLength: 3 }),
  revive: fc.integer({ min: 0, max: 2 }),
})

/** The quiet half of the space: nobody cancels, nobody arrives late, all resolve. */
const plainStepArbitrary: fc.Arbitrary<Step> = stepArbitrary.map((step) => ({
  ...step,
  loader: 'resolve',
  cancel: [false, false, false],
  revive: 0,
}))

const NO_FAULTS: FaultPlan = {
  acquire: null,
  waitForChange: null,
  renew: null,
  complete: null,
  abandon: null,
  get: null,
  set: null,
}

const scenarioArbitrary: fc.Arbitrary<Scenario> = fc.record({
  transcript: fc.array(stepArbitrary, { minLength: 1, maxLength: 3 }),
  faults: fc.record({
    acquire: fc.option(fc.integer({ min: 0, max: 6 }), { nil: null }),
    waitForChange: fc.option(fc.integer({ min: 0, max: 6 }), { nil: null }),
    renew: fc.option(fc.integer({ min: 0, max: 6 }), { nil: null }),
    complete: fc.option(fc.integer({ min: 0, max: 6 }), { nil: null }),
    abandon: fc.option(fc.integer({ min: 0, max: 6 }), { nil: null }),
    get: fc.option(fc.integer({ min: 0, max: 6 }), { nil: null }),
    set: fc.option(fc.integer({ min: 0, max: 6 }), { nil: null }),
  }),
  failureMode: fc.constantFrom<CoordinationFailureMode>(
    'fail-closed',
    'fail-open'
  ),
})

const tick = (): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, 0))

interface Deferred {
  promise: Promise<void>
  resolve: () => void
}

const deferred = (): Deferred => {
  let resolve!: () => void
  const promise = new Promise<void>((res) => {
    resolve = res
  })

  return { promise, resolve }
}

/** A flight that hangs is a finding of its own, so every call is bounded. */
const withinDeadline = async <Ts>(work: Promise<Ts>): Promise<Ts> => {
  let timer: ReturnType<typeof setTimeout> | undefined
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`did not settle within ${SETTLE_TIMEOUT_MS}ms`)),
      SETTLE_TIMEOUT_MS
    )
  })

  try {
    return await Promise.race([work, deadline])
  } finally {
    clearTimeout(timer)
  }
}

/** Yields until the predicate holds, or the budget runs out. */
const settles = async (
  predicate: () => boolean,
  timeoutMs = 500
): Promise<boolean> => {
  const deadline = Date.now() + timeoutMs

  while (!predicate() && Date.now() < deadline) {
    await tick()
  }

  return predicate()
}

/** One counter per command kind, so a plan can name the nth call that fails. */
const counted = (): (() => number) => {
  let count = 0

  return () => {
    const current = count
    count += 1
    return current
  }
}

/** The faults a run actually hit: a plan may name an index the run never reaches. */
type FiredFaults = Record<keyof FaultPlan, boolean>

interface Faults {
  acquire: () => boolean
  waitForChange: () => boolean
  renew: () => boolean
  complete: () => boolean
  abandon: () => boolean
  get: () => boolean
  set: () => boolean
  /** Which of the above fired, for the assertions that have to tell them apart. */
  fired: FiredFaults
}

const faultsFor = (plan: FaultPlan): Faults => {
  const counters: Record<keyof FaultPlan, () => number> = {
    acquire: counted(),
    waitForChange: counted(),
    renew: counted(),
    complete: counted(),
    abandon: counted(),
    get: counted(),
    set: counted(),
  }

  const fired: FiredFaults = {
    acquire: false,
    waitForChange: false,
    renew: false,
    complete: false,
    abandon: false,
    get: false,
    set: false,
  }

  // `fired` is set where the fault is thrown, not where the plan names it: an
  // index the run never reaches is not a failure and may not excuse anything.
  const due = (kind: keyof FaultPlan) => (): boolean => {
    const index = plan[kind]

    if (index === null || counters[kind]() !== index) {
      return false
    }

    fired[kind] = true
    return true
  }

  return {
    acquire: due('acquire'),
    waitForChange: due('waitForChange'),
    renew: due('renew'),
    complete: due('complete'),
    abandon: due('abandon'),
    get: due('get'),
    set: due('set'),
    fired,
  }
}

const faultyLease = (lease: Lease, faults: Faults): Lease => ({
  get key(): string {
    return lease.key
  },
  async renew(): Promise<boolean> {
    if (faults.renew()) {
      throw new Error('renew failed')
    }

    return lease.renew()
  },
  async complete(): Promise<void> {
    if (faults.complete()) {
      throw new Error('complete failed')
    }

    return lease.complete()
  },
  async abandon(): Promise<void> {
    if (faults.abandon()) {
      throw new Error('abandon failed')
    }

    return lease.abandon()
  },
})

const faultyCoordinator = (
  inner: InMemoryCoordinator,
  faults: Faults
): Coordinator => ({
  async acquire(key, options) {
    if (faults.acquire()) {
      throw new Error('acquire failed')
    }

    const lease = await inner.acquire(key, options)
    return lease === null ? null : faultyLease(lease, faults)
  },
  async waitForChange(key, options) {
    if (faults.waitForChange()) {
      throw new Error('waitForChange failed')
    }

    return inner.waitForChange(key, options)
  },
  async close() {
    return inner.close()
  },
})

const faultyStore = (store: StoreHarness, faults: Faults): CacheAdapter => ({
  async get<T>(key: string): Promise<CacheLookup<T>> {
    if (faults.get()) {
      throw new Error('cache read failed')
    }

    return store.adapter.get<T>(key)
  },
  async set<T>(
    key: string,
    value: T,
    options?: CacheSetOptions
  ): Promise<void> {
    if (faults.set()) {
      throw new Error('cache write failed')
    }

    return store.adapter.set(key, value, options)
  },
})

interface Caller {
  promise: Promise<unknown>
  controller: AbortController | undefined
  reason: Error | undefined
  settled: boolean
  /** Cancelled while still pending: its own reason is what it must reject with. */
  cancelledWhilePending: boolean
}

interface StepResult {
  key: string
  callers: number
  runs: number
  cancelled: number
  revived: number
  rejections: number
  /** The values served in this step, in caller order. */
  values: unknown[]
}

interface ScenarioResult {
  /** Everything that must never happen, described so a failure names it. */
  violations: string[]
  steps: StepResult[]
  /** Leases still held once the transcript had settled, before close(). */
  leaked: number
  /** The faults the run hit, so a leak is only excused by a release that failed. */
  fired: FiredFaults
}

/**
 * Runs one transcript against a fresh instance, a fresh store and a fresh
 * coordinator, and reports what it observed. Nothing is asserted here: the
 * properties decide which part of the report they are about, so one run of the
 * scenario can be read as invariants (with faults) or as a sharp oracle (without
 * them).
 */
const runScenario = async (scenario: Scenario): Promise<ScenarioResult> => {
  const store = memoryStore()
  const faults = faultsFor(scenario.faults)
  const coordinator = new InMemoryCoordinator()
  const violations: string[] = []
  const produced = new Map<string, Set<unknown>>()
  const steps: StepResult[] = []

  const crossflight = createCrossflight({
    cache: faultyStore(store, faults),
    coordinator: faultyCoordinator(coordinator, faults),
    // A contended key has to give up quickly here: the default retry budget
    // would spend seconds on a lease a failed release left behind.
    maxRetryAttempts: 2,
    retryBackoff: () => 5,
    failureMode: scenario.failureMode,
  })

  // The loader that is live for a key, so a second live one can be spotted.
  const live = new Map<string, AbortSignal>()

  try {
    for (const step of scenario.transcript) {
      const gate = deferred()
      const values = produced.get(step.key) ?? new Set<unknown>()
      produced.set(step.key, values)
      let runs = 0

      const loader = async (signal: AbortSignal): Promise<unknown> => {
        const running = live.get(step.key)

        // Overlapping a winding-down loader is the one case that is allowed, and
        // only because its own signal has already been aborted: while a loader's
        // signal is live, no second loader for the key may start. That is the
        // stampede protection, and this is where it is checked.
        if (running !== undefined && !running.aborted) {
          violations.push(`two live loaders for "${step.key}"`)
        }

        live.set(step.key, signal)
        runs += 1

        try {
          await gate.promise

          if (step.loader === 'reject') {
            throw new Error('loader failed')
          }

          values.add(step.value)
          return step.value
        } finally {
          if (live.get(step.key) === signal) {
            live.delete(step.key)
          }
        }
      }

      const group: Caller[] = []
      const start = (cancellable: boolean): Caller => {
        const controller = cancellable ? new AbortController() : undefined
        const reason = cancellable
          ? new Error(`caller ${group.length} cancelled`)
          : undefined

        const caller: Caller = {
          promise: crossflight.wrap(
            step.key,
            loader,
            controller ? { signal: controller.signal } : undefined
          ),
          controller,
          reason,
          settled: false,
          cancelledWhilePending: false,
        }

        // Both handlers: following a settling caller must not turn its rejection
        // into an unhandled one.
        caller.promise.then(
          () => {
            caller.settled = true
          },
          () => {
            caller.settled = true
          }
        )

        group.push(caller)
        return caller
      }

      const original = Array.from({ length: step.callers }, (_, index) =>
        start(step.cancel[index] ?? false)
      )

      // Handlers first: the cancellations below reject in this same task, and a
      // rejection nobody is attached to yet is reported as unhandled.
      const originalSettled = Promise.allSettled(
        original.map((caller) => caller.promise)
      )

      const aborted: unknown[] = []
      for (const caller of original) {
        if (caller.controller === undefined) {
          continue
        }

        aborted.push(caller.reason)
        caller.cancelledWhilePending = !caller.settled
        caller.controller.abort(caller.reason)
      }

      await tick()

      const revived = Array.from({ length: step.revive }, () => start(false))
      const revivedSettled = Promise.allSettled(
        revived.map((caller) => caller.promise)
      )

      await tick()
      gate.resolve()

      const [originalOutcomes, revivedOutcomes] = await withinDeadline(
        Promise.all([originalSettled, revivedSettled])
      )

      let rejections = 0
      const served: unknown[] = []

      const collect = (
        caller: Caller,
        outcome: PromiseSettledResult<unknown>
      ): void => {
        if (outcome.status === 'fulfilled') {
          if (!values.has(outcome.value)) {
            violations.push(
              `a caller received a value no loader produced for "${step.key}"`
            )
          }

          served.push(outcome.value)
          return
        }

        rejections += 1

        if (caller.cancelledWhilePending) {
          if (outcome.reason !== caller.reason) {
            violations.push('a cancelled caller rejected with another reason')
          }

          return
        }

        if (aborted.includes(outcome.reason)) {
          violations.push(
            'a caller that did not cancel rejected with a cancellation reason'
          )
        }
      }

      original.forEach((caller, index) => {
        collect(caller, originalOutcomes[index]!)
      })
      revived.forEach((caller, index) => {
        collect(caller, revivedOutcomes[index]!)
      })

      // Let winding-down loaders finish before the next step reads `live`.
      await tick()
      await tick()

      steps.push({
        key: step.key,
        callers: step.callers,
        runs,
        cancelled: aborted.length,
        revived: revived.length,
        rejections,
        values: served,
      })
    }

    // Cleanup that a settled flight starts in the background needs a turn to
    // land before the leases it holds can be counted.
    await settles(() => coordinator.owners.size === 0)

    return {
      violations,
      steps,
      leaked: coordinator.owners.size,
      fired: faults.fired,
    }
  } finally {
    await crossflight.close()
  }
}

describe('a generated transcript', () => {
  itProperty(
    'holds its invariants however the calls arrive and the backend fails',
    scenarioArbitrary,
    async (scenario) => {
      const result = await runScenario(scenario)

      // Mutual exclusion, cancellation isolation, value provenance and the
      // settling of every call, in one report: the entries name what broke.
      expect(result.violations).toEqual([])

      // A release that actually failed may leave its lease to the ttl - that is
      // what the ttl is for. A plan that merely named an index the run never
      // reached is not a failed release, so it excuses nothing.
      if (!result.fired.complete && !result.fired.abandon) {
        expect(result.leaked).toBe(0)
      }
    }
  )

  itProperty(
    'serves a step from one loader run when nothing fails and nobody cancels',
    fc.array(plainStepArbitrary, { minLength: 1, maxLength: 3 }),
    async (transcript) => {
      const result = await runScenario({
        transcript,
        faults: NO_FAULTS,
        failureMode: 'fail-closed',
      })

      expect(result.violations).toEqual([])
      expect(result.leaked).toBe(0)

      for (const step of result.steps) {
        // Nothing failed and nobody left, so every caller is served.
        expect(step.rejections).toBe(0)
        expect(step.values).toHaveLength(step.callers)

        // And served without a stampede: a second loader run for the step would
        // be one, whether or not it produced the same value.
        expect(step.runs).toBeLessThanOrEqual(1)

        // Whether the group was served from a run or from a value a previous
        // step cached, it was one value for all of them: they shared a flight.
        expect(new Set(step.values).size).toBe(1)
      }
    }
  )
})
