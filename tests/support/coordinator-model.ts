import * as fc from 'fast-check'
import { vi } from 'vitest'

import type { Coordinator, Lease } from '../../src/types.js'

/**
 * The coordinator contract of docs/creating-a-coordinator.md, written as a
 * reference *model*, plus the machinery to run one generated scenario against
 * the model and against a real coordinator and compare what the two did.
 *
 * The contract suite pins the documented behaviours one case at a time, which
 * cannot cover how expiry, ownership and release order interact. A model can be
 * run over hundreds of orderings, and - unlike a hand-written expectation - it
 * is an independent statement of the contract, so an implementation that drifts
 * from it fails instead of redefining what the test expects.
 *
 * Time is the one thing the two runs cannot share: a Redis coordinator expires
 * a lease against Redis's own clock, while the model is advanced by the same
 * scenario. A clock therefore *realises* a scenario's jumps - a backend that can
 * only expire everything at once rewrites each jump to that larger one - and the
 * model, which reads the realised scenario, ends up in the same state.
 *
 * Waits are in the scenario too: a `waitForChange` is started where the scenario
 * asks for it and read once every other operation has happened, so a change the
 * scenario announces is what could have woken it early. The contract is only
 * that a wait settles - early on a change, or at its own timeout - so the model
 * expects `resolved`, and a rejection is a difference the comparison reports.
 */

export type ReleaseMode = 'complete' | 'abandon'

export interface ModelLease {
  readonly key: string
  readonly token: number
  readonly ttlMs: number
}

export type Operation =
  | { kind: 'advance'; byMs: number }
  | { kind: 'acquire'; key: string; id: number; ttlMs: number }
  | { kind: 'renew'; key: string; id: number }
  | { kind: 'release'; key: string; id: number; mode: ReleaseMode }
  | { kind: 'wait'; key: string; id: number; timeoutMs: number }

/**
 * What one operation did, in terms the model can produce too. `no-lease` means
 * the run holds no lease for that id - the acquisition was refused - which is
 * different from renewing a lease that has since been lost (`renewed: false`).
 */
export type Observation =
  | { kind: 'advance'; byMs: number }
  | { kind: 'acquire'; key: string; id: number; acquired: boolean }
  | { kind: 'renew'; key: string; id: number; renewed: boolean }
  | { kind: 'release'; key: string; id: number; mode: ReleaseMode }
  | {
      kind: 'wait'
      key: string
      id: number
      /** A wait settles on a change or at its own timeout; never by hanging. */
      settled: 'resolved' | 'rejected'
      /** Empty when the wait resolved, the error when the coordinator rejected. */
      reason: string
    }
  | {
      kind: 'no-lease'
      operation: 'renew' | 'release'
      key: string
      id: number
    }
  | { kind: 'error'; operation: string; message: string }

/**
 * The contract as state. Every rule is a quote from the documentation: a lease is
 * owned by key, a token identifies its owner, and only the ttl takes it away.
 */
export class CoordinatorModel {
  private readonly owned = new Map<
    string,
    { token: number; expiresAt: number }
  >()

  private token = 0

  constructor(
    private nowMs = 0,
    private readonly defaultTtlMs = 30_000
  ) {}

  advance(byMs: number): void {
    this.nowMs += byMs
  }

  acquire(key: string, ttlMs?: number): ModelLease | null {
    const current = this.owned.get(key)

    // "Once TTL passes, the lease is invalid. Another caller can immediately
    // acquire it." Until then the owner is the only one who can hold it.
    if (current !== undefined && current.expiresAt > this.nowMs) {
      return null
    }

    const ttl = ttlMs ?? this.defaultTtlMs
    const token = (this.token += 1)
    this.owned.set(key, { token, expiresAt: this.nowMs + ttl })

    return { key, token, ttlMs: ttl }
  }

  renew(lease: ModelLease): boolean {
    const current = this.owned.get(lease.key)

    // "Never act on a key whose owner has changed."
    if (current === undefined || current.token !== lease.token) {
      return false
    }

    // An expired lease is gone: it cannot be renewed back to life.
    if (current.expiresAt <= this.nowMs) {
      this.owned.delete(lease.key)
      return false
    }

    current.expiresAt = this.nowMs + lease.ttlMs
    return true
  }

  release(lease: ModelLease): void {
    const current = this.owned.get(lease.key)

    if (current !== undefined && current.token === lease.token) {
      this.owned.delete(lease.key)
    }
  }

  /** Whether the key is owned right now: what a stale release must not change. */
  held(key: string): boolean {
    const current = this.owned.get(key)
    return current !== undefined && current.expiresAt > this.nowMs
  }
}

/** A clock an implementation reads, and how a scenario's jumps are realised. */
export interface Clock {
  /**
   * Rewrites the scenario's time jumps to the ones this backend can actually
   * make: a virtual clock moves time exactly and changes nothing, while a
   * backend that can only expire everything at once rewrites each jump to that
   * larger one. The model runs the rewritten scenario too, so both sides agree
   * about what time did.
   */
  realise(operations: Operation[]): Operation[]
  /** Moves the implementation's clock by an already realised jump. */
  advance(byMs: number): Promise<void>
}

/** A jump that outlives any ttl a scenario can acquire a lease with. */
export const EXPIRE_EVERYTHING_MS = 60_000

export interface Participant {
  name: string
  /** The key this backend is asked for, given a scenario's logical key. */
  key(logicalKey: string): string
  create(): Promise<Coordinator> | Coordinator
  clock: Clock
  /**
   * The smallest ttl this backend honours unambiguously. A lease that expires on
   * its own mid-scenario - because a round trip outlasted it - would make the
   * two runs disagree about real time, so every ttl is raised to at least this.
   */
  minTtlMs?: number
}

/**
 * Exact, frozen time: the in-memory coordinators read `Date.now()` and nothing
 * else, so faking only `Date` moves their notion of expiry while their waits
 * keep real timers. That split is what lets an expiry boundary be tested without
 * waiting for it.
 */
export const virtualClock = (
  startMs = 1_000_000
): Clock & { dispose: () => void } => {
  vi.useFakeTimers({ toFake: ['Date'] })
  let nowMs = startMs
  vi.setSystemTime(nowMs)

  return {
    realise: (operations) => operations,
    async advance(byMs: number): Promise<void> {
      nowMs += byMs
      vi.setSystemTime(nowMs)
    },
    dispose: () => {
      vi.useRealTimers()
    },
  }
}

export const SCENARIO_KEYS = ['first', 'second'] as const

export type RawOperation =
  | { kind: 'advance'; byMs: number }
  | { kind: 'acquire'; key: string; ttlMs: number }
  | { kind: 'renew'; handle: number }
  | { kind: 'release'; handle: number; mode: ReleaseMode }
  /** A wait on the key of the nth acquisition. */
  | { kind: 'wait'; handle: number; timeoutMs: number }
  /**
   * A wait on a key of the scenario's own: a caller may wait for a change before
   * it holds anything, and the model has to expect the same wake-up then.
   */
  | { kind: 'wait-key'; key: string; timeoutMs: number }

export interface Scenario {
  operations: Operation[]
}

/**
 * Names the acquisitions a scenario refers to: a renewal, a release or a wait is
 * written against the nth acquisition, which is how a scenario can renew a lease
 * whose acquisition was refused, release one whose owner has since changed, or
 * wait on a key that was never taken. A wait may instead name a key of its own
 * (`wait-key`), which is a caller waiting - as a caller may - before it has taken
 * anything.
 *
 * Every scenario ends with a jump past every lease and one acquisition per key it
 * touched - a key a wait named included, so the change that closes the scenario is
 * announced for that key too. A lease the implementation left behind but the model
 * released shows up as a difference in those last observations.
 */
export const materialize = (raw: RawOperation[]): Scenario => {
  const acquired: Array<{ key: string; id: number }> = []
  const touched: string[] = []
  const operations: Operation[] = []
  // The identity of the waits that name no acquisition: negative, so it cannot
  // collide with an acquisition's id and the order both sides read their waits
  // in is the same.
  let standaloneWaits = 0

  for (const operation of raw) {
    if (operation.kind === 'advance') {
      operations.push(operation)
      continue
    }

    if (operation.kind === 'acquire') {
      if (!touched.includes(operation.key)) {
        touched.push(operation.key)
      }

      const id = acquired.length
      operations.push({ ...operation, id })
      acquired.push({ key: operation.key, id })
      continue
    }

    if (operation.kind === 'wait-key') {
      // Waiting for a key nothing has taken yet is a wait like any other: it is
      // the key that is touched, and the scenario's closing acquisition of it is
      // what announces the change a wake-up would need.
      if (!touched.includes(operation.key)) {
        touched.push(operation.key)
      }

      operations.push({
        kind: 'wait',
        key: operation.key,
        id: -1 - standaloneWaits,
        timeoutMs: operation.timeoutMs,
      })
      standaloneWaits += 1
      continue
    }

    if (acquired.length === 0) {
      continue
    }

    const target = acquired[operation.handle % acquired.length]!

    if (operation.kind === 'renew') {
      operations.push({ kind: 'renew', key: target.key, id: target.id })
      continue
    }

    if (operation.kind === 'release') {
      operations.push({
        kind: 'release',
        key: target.key,
        id: target.id,
        mode: operation.mode,
      })
      continue
    }

    // A wait is written against an acquisition, and waits on that key: the
    // scenario is what gives it a key and an identity the model can match.
    operations.push({
      kind: 'wait',
      key: target.key,
      id: target.id,
      timeoutMs: operation.timeoutMs,
    })
  }

  operations.push({ kind: 'advance', byMs: EXPIRE_EVERYTHING_MS })

  for (const key of touched.length === 0 ? SCENARIO_KEYS : touched) {
    const id = acquired.length
    operations.push({ kind: 'acquire', key, id, ttlMs: 5_000 })
    acquired.push({ key, id })
  }

  return { operations }
}

const advanceArbitrary: fc.Arbitrary<RawOperation> = fc.record({
  kind: fc.constant('advance' as const),
  // Including the exact boundary: 0, and jumps that land on a ttl.
  byMs: fc.constantFrom(0, 5, 10, 25, 60, 200),
})

const acquireArbitrary: fc.Arbitrary<RawOperation> = fc.record({
  kind: fc.constant('acquire' as const),
  key: fc.constantFrom(...SCENARIO_KEYS),
  ttlMs: fc.constantFrom(10, 25, 60),
})

const renewArbitrary: fc.Arbitrary<RawOperation> = fc.record({
  kind: fc.constant('renew' as const),
  handle: fc.nat({ max: 5 }),
})

const releaseArbitrary: fc.Arbitrary<RawOperation> = fc.record({
  kind: fc.constant('release' as const),
  handle: fc.nat({ max: 5 }),
  mode: fc.constantFrom('complete' as const, 'abandon' as const),
})

/**
 * A wait of a few milliseconds: what matters is that it settles, and that a
 * change announced while it is parked may settle it earlier. Longer windows
 * would only make the suite slower, not the scenario different.
 *
 * Half the waits name an acquisition and half a key: a caller waiting before it
 * owns anything is a wait the contract has to answer too, and it settles the same
 * way - on the change the scenario's last acquisition of that key announces.
 */
const waitArbitrary: fc.Arbitrary<RawOperation> = fc.record({
  kind: fc.constant('wait' as const),
  handle: fc.nat({ max: 5 }),
  timeoutMs: fc.constantFrom(5, 10),
})

const waitKeyArbitrary: fc.Arbitrary<RawOperation> = fc.record({
  kind: fc.constant('wait-key' as const),
  key: fc.constantFrom(...SCENARIO_KEYS),
  timeoutMs: fc.constantFrom(5, 10),
})

export const scenarioArbitrary: fc.Arbitrary<Scenario> = fc
  .array(
    fc.oneof(
      advanceArbitrary,
      acquireArbitrary,
      renewArbitrary,
      releaseArbitrary,
      waitArbitrary,
      waitKeyArbitrary
    ),
    { minLength: 1, maxLength: 14 }
  )
  .map(materialize)

/** What the contract says the run should do, from the same scenario. */
export const modelObservations = (
  scenario: Scenario,
  minTtlMs = 0
): Observation[] => {
  const model = new CoordinatorModel()
  const leases = new Map<number, ModelLease>()
  const observations: Observation[] = []

  // A wait is started by its operation and read once every other operation has
  // happened: reading it there is what lets a change settle it early, and both
  // sides append the outcomes in the same order.
  const pendingWaits: Array<{ key: string; id: number }> = []

  for (const operation of scenario.operations) {
    switch (operation.kind) {
      case 'advance': {
        model.advance(operation.byMs)
        observations.push(operation)
        break
      }

      case 'acquire': {
        const lease = model.acquire(
          operation.key,
          Math.max(operation.ttlMs, minTtlMs)
        )

        if (lease !== null) {
          leases.set(operation.id, lease)
        }

        observations.push({
          kind: 'acquire',
          key: operation.key,
          id: operation.id,
          acquired: lease !== null,
        })
        break
      }

      case 'renew': {
        const lease = leases.get(operation.id)

        // A handle outlives its lease: renewing one that was released or taken
        // over reports `false`, and only an id that never held a lease at all is
        // reported as `no-lease`.
        observations.push(
          lease === undefined
            ? {
                kind: 'no-lease',
                operation: 'renew',
                key: operation.key,
                id: operation.id,
              }
            : {
                kind: 'renew',
                key: operation.key,
                id: operation.id,
                renewed: model.renew(lease),
              }
        )
        break
      }

      case 'release': {
        const lease = leases.get(operation.id)

        if (lease === undefined) {
          observations.push({
            kind: 'no-lease',
            operation: 'release',
            key: operation.key,
            id: operation.id,
          })
          break
        }

        model.release(lease)
        observations.push({
          kind: 'release',
          key: operation.key,
          id: operation.id,
          mode: operation.mode,
        })
        break
      }

      case 'wait': {
        // The outcome is not known yet, and the contract cannot see it: the
        // change that may settle this wait early has not happened.
        pendingWaits.push({ key: operation.key, id: operation.id })
        break
      }
    }
  }

  // A healthy coordinator always settles a wait - early on a change, or at its
  // own timeout - so a rejection in the implementation's list is a difference
  // these two lists are here to report.
  for (const wait of [...pendingWaits].sort(
    (left, right) => left.id - right.id
  )) {
    observations.push({
      kind: 'wait',
      key: wait.key,
      id: wait.id,
      settled: 'resolved',
      reason: '',
    })
  }

  return observations
}

const describeError = (error: unknown): string =>
  error instanceof Error ? `${error.name}: ${error.message}` : String(error)

/** What the implementation actually does with the same scenario. */
export const implementationObservations = async (
  scenario: Scenario,
  participant: Participant,
  coordinator: Coordinator
): Promise<Observation[]> => {
  const leases = new Map<number, Lease>()
  const observations: Observation[] = []
  const minTtlMs = participant.minTtlMs ?? 0

  // Started by their operation, read once everything else has happened - the
  // same shape the model uses, so the two lists stay aligned.
  const pendingWaits: Array<{
    key: string
    id: number
    outcome: Promise<{ settled: 'resolved' | 'rejected'; reason: string }>
  }> = []

  for (const operation of scenario.operations) {
    try {
      switch (operation.kind) {
        case 'advance': {
          await participant.clock.advance(operation.byMs)
          observations.push(operation)
          break
        }

        case 'acquire': {
          const lease = await coordinator.acquire(
            participant.key(operation.key),
            { ttlMs: Math.max(operation.ttlMs, minTtlMs) }
          )

          if (lease !== null) {
            leases.set(operation.id, lease)
          }

          observations.push({
            kind: 'acquire',
            key: operation.key,
            id: operation.id,
            acquired: lease !== null,
          })
          break
        }

        case 'renew': {
          const lease = leases.get(operation.id)

          observations.push(
            lease === undefined
              ? {
                  kind: 'no-lease',
                  operation: 'renew',
                  key: operation.key,
                  id: operation.id,
                }
              : {
                  kind: 'renew',
                  key: operation.key,
                  id: operation.id,
                  renewed: await lease.renew(),
                }
          )
          break
        }

        case 'release': {
          const lease = leases.get(operation.id)

          if (lease === undefined) {
            observations.push({
              kind: 'no-lease',
              operation: 'release',
              key: operation.key,
              id: operation.id,
            })
            break
          }

          // The handle stays in hand, as it does in the model: a released lease
          // still has to answer `renew()` with `false`.
          if (operation.mode === 'complete') {
            await lease.complete()
          } else {
            await lease.abandon()
          }

          observations.push({
            kind: 'release',
            key: operation.key,
            id: operation.id,
            mode: operation.mode,
          })
          break
        }

        case 'wait': {
          // Started and not awaited: what settles a wait early is a change that
          // has not been announced yet. Both handlers are attached here, so a
          // rejection becomes an observation instead of an unhandled one.
          pendingWaits.push({
            key: operation.key,
            id: operation.id,
            outcome: coordinator
              .waitForChange(participant.key(operation.key), {
                timeoutMs: operation.timeoutMs,
              })
              .then(
                (): { settled: 'resolved'; reason: string } => ({
                  settled: 'resolved',
                  reason: '',
                }),
                (error): { settled: 'rejected'; reason: string } => ({
                  settled: 'rejected',
                  reason: describeError(error),
                })
              ),
          })
          break
        }
      }
    } catch (error) {
      observations.push({
        kind: 'error',
        operation: operation.kind,
        message: describeError(error),
      })
    }
  }

  // Read the waits once everything else has happened: a change the operations
  // above announced is what a wake-up would have been.
  for (const wait of [...pendingWaits].sort(
    (left, right) => left.id - right.id
  )) {
    observations.push({
      kind: 'wait',
      key: wait.key,
      id: wait.id,
      ...(await wait.outcome),
    })
  }

  return observations
}

export interface Run {
  participant: string
  /** What the implementation did. */
  actual: Observation[]
  /** What the contract - the model - says it should have done. */
  expected: Observation[]
}

/**
 * Runs one scenario twice - once against the model and once against the
 * participant's coordinator, both from the same realised scenario - and hands
 * the caller the two observation lists to compare.
 */
export const runScenario = async (
  scenario: Scenario,
  participant: Participant
): Promise<Run> => {
  const operations = participant.clock.realise(scenario.operations)
  const planned: Scenario = { operations }
  const coordinator = await participant.create()

  try {
    return {
      participant: participant.name,
      actual: await implementationObservations(
        planned,
        participant,
        coordinator
      ),
      expected: modelObservations(planned, participant.minTtlMs ?? 0),
    }
  } finally {
    await coordinator.close()
  }
}
