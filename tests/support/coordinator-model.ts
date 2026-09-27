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

export interface Scenario {
  operations: Operation[]
}

/**
 * Names the acquisitions a scenario refers to: a renewal or a release is written
 * against the nth acquisition, which is how a scenario can renew a lease whose
 * acquisition was refused, or release one whose owner has since changed.
 *
 * Every scenario ends with a jump past every lease and one acquisition per key
 * it touched. A lease the implementation left behind but the model released
 * shows up as a difference in those last observations.
 */
export const materialize = (raw: RawOperation[]): Scenario => {
  const acquired: Array<{ key: string; id: number }> = []
  const touched: string[] = []
  const operations: Operation[] = []

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

    if (acquired.length === 0) {
      continue
    }

    const target = acquired[operation.handle % acquired.length]!
    operations.push(
      operation.kind === 'renew'
        ? { kind: 'renew', key: target.key, id: target.id }
        : {
            kind: 'release',
            key: target.key,
            id: target.id,
            mode: operation.mode,
          }
    )
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

export const scenarioArbitrary: fc.Arbitrary<Scenario> = fc
  .array(
    fc.oneof(
      advanceArbitrary,
      acquireArbitrary,
      renewArbitrary,
      releaseArbitrary
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
    }
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
      }
    } catch (error) {
      observations.push({
        kind: 'error',
        operation: operation.kind,
        message: describeError(error),
      })
    }
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
