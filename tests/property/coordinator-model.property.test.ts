import { describe, expect, it } from 'vitest'

import type { Coordinator, Lease } from '../../src/types.js'
import { EventedMemoryCoordinator } from '../mocks/evented-memory-coordinator.js'
import { InMemoryCoordinator } from '../mocks/in-memory-coordinator.js'
import {
  runScenario,
  scenarioArbitrary,
  virtualClock,
  type Clock,
  type Participant,
  type Scenario,
} from '../support/coordinator-model.js'
import { createPropertySuite } from '../support/seed.js'

/**
 * The coordinator contract, differentially: one generated scenario - acquires,
 * renewals, releases and time jumps in any order - is run against the reference
 * model and against a coordinator, and the two observation lists have to be
 * equal. The contract suite checks the documented behaviours one at a time; this
 * checks that no *combination* of them drifts, including the orderings nobody
 * would think to write down: releasing another owner's lease, renewing after a
 * takeover, reacquiring at the exact millisecond a ttl passes.
 *
 * The model is the contract, not a copy of an implementation, and the last test
 * in this file is the proof that the comparison can fail: a coordinator that
 * gets one rule wrong is flagged.
 */
const itProperty = createPropertySuite('coordinator-model', { runs: 120 })

const participant = (
  name: string,
  clock: Clock,
  create: () => Coordinator
): Participant => ({
  name,
  // The in-memory doubles take the logical key as it is.
  key: (logicalKey) => logicalKey,
  create,
  clock,
})

const implementations: Array<{ name: string; create: () => Coordinator }> = [
  {
    name: 'in-memory coordinator double',
    create: () => new InMemoryCoordinator(),
  },
  {
    name: 'evented memory coordinator',
    create: () => new EventedMemoryCoordinator(),
  },
]

describe('the coordinator contract as a model', () => {
  itProperty(
    'agrees with every in-memory coordinator over generated lease sequences',
    scenarioArbitrary,
    async (scenario) => {
      const clock = virtualClock()

      try {
        for (const implementation of implementations) {
          const run = await runScenario(
            scenario,
            participant(implementation.name, clock, implementation.create)
          )

          expect(run.actual, implementation.name).toEqual(run.expected)
        }
      } finally {
        clock.dispose()
      }
    }
  )

  it('holds the boundary the model encodes: a lease is gone exactly at its ttl', async () => {
    const clock = virtualClock()

    try {
      for (const implementation of implementations) {
        const subject = implementation.create()
        const lease = await subject.acquire('boundary', { ttlMs: 100 })
        expect(lease, implementation.name).not.toBeNull()

        // One millisecond short of the ttl the owner still holds the key.
        await clock.advance(99)
        expect(
          await subject.acquire('boundary', { ttlMs: 100 }),
          implementation.name
        ).toBeNull()

        // At exactly the ttl it does not: `expiresAt <= now` is expiry, and the
        // model treats it the same way, so a drift in either is a mismatch.
        await clock.advance(1)
        expect(await lease!.renew(), implementation.name).toBe(false)
        expect(
          await subject.acquire('boundary', { ttlMs: 100 }),
          implementation.name
        ).not.toBeNull()

        await subject.close()
      }
    } finally {
      clock.dispose()
    }
  })

  it('freezes Date without freezing the real timers a wait depends on', async () => {
    const clock = virtualClock()

    try {
      const before = Date.now()
      await new Promise((resolve) => setTimeout(resolve, 5))

      // The wait above settled, and time did not move on its own.
      expect(Date.now()).toBe(before)

      await clock.advance(250)
      expect(Date.now()).toBe(before + 250)
    } finally {
      clock.dispose()
    }
  })

  it('flags a coordinator that never lets a lease expire', async () => {
    // The differential is only worth its runtime if it can fail. This
    // coordinator breaks exactly one rule - a lease it granted is held forever -
    // and the scenario that catches it is the smallest one that can: take a
    // lease, move past its ttl, take it again.
    const immortal = (): Coordinator => {
      const owners = new Map<string, number>()
      let token = 0

      return {
        async acquire(key: string): Promise<Lease | null> {
          if (owners.has(key)) {
            return null
          }

          const mine = ++token
          owners.set(key, mine)

          return {
            key,
            renew: async () => owners.get(key) === mine,
            complete: async () => {
              owners.delete(key)
            },
            abandon: async () => {
              owners.delete(key)
            },
          }
        },
        async waitForChange(): Promise<void> {},
        async close(): Promise<void> {
          owners.clear()
        },
      }
    }

    const scenario: Scenario = {
      operations: [
        { kind: 'acquire', key: 'first', id: 0, ttlMs: 10 },
        { kind: 'advance', byMs: 50 },
        { kind: 'acquire', key: 'first', id: 1, ttlMs: 10 },
      ],
    }

    const clock = virtualClock()

    try {
      const run = await runScenario(
        scenario,
        participant('immortal', clock, immortal)
      )

      // The model lets the key go after the jump; this coordinator still holds
      // it, so the second acquisition is refused where the model grants it.
      expect(run.actual).not.toEqual(run.expected)
      expect(run.expected.at(-1)).toEqual({
        kind: 'acquire',
        key: 'first',
        id: 1,
        acquired: true,
      })
      expect(run.actual.at(-1)).toEqual({
        kind: 'acquire',
        key: 'first',
        id: 1,
        acquired: false,
      })
    } finally {
      clock.dispose()
    }
  })
})
