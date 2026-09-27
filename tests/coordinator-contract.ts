import { describe, expect, it } from 'vitest'

import type { Coordinator } from '../src/types.js'

export interface CoordinatorContractOptions {
  /** Registers the suite as skipped when false (e.g. Redis integration is off). */
  run?: boolean
  /** Wakes waiters when ownership changes: acquire, renew, complete, abandon. */
  notifications?: boolean
  /** Rejects acquire() and waitForChange() once close() has been called. */
  closesOperations?: boolean
}

const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms))

/**
 * The executable form of the coordinator contract described in
 * docs/creating-a-coordinator.md. Run it against every implementation so that a
 * coordinator which drifts from the documented behaviour fails loudly instead of
 * silently.
 */
export function runCoordinatorContract(
  name: string,
  create: () => Coordinator | Promise<Coordinator>,
  options: CoordinatorContractOptions = {}
): void {
  const prefix = name.replace(/[^a-z0-9]+/gi, '-').toLowerCase()
  const key = (suffix: string): string => `contract:${prefix}:${suffix}`

  describe.runIf(options.run ?? true)(`coordinator contract: ${name}`, () => {
    it('grants a lease for a free key and reports a stable lease identifier', async () => {
      const coordinator = await create()
      const first = await coordinator.acquire(key('identity-a'), { ttlMs: 500 })
      const second = await coordinator.acquire(key('identity-b'), { ttlMs: 500 })

      expect(first).not.toBeNull()
      expect(second).not.toBeNull()

      // The lease reports the coordinator's own identifier for it: that may be
      // namespaced or hashed, so only its shape and stability are contractual.
      const leaseKey = first!.key
      expect(typeof leaseKey).toBe('string')
      expect(leaseKey.length).toBeGreaterThan(0)
      expect(second!.key).not.toBe(leaseKey)

      await expect(first!.renew()).resolves.toBe(true)
      expect(first!.key).toBe(leaseKey)

      await first!.complete()
      await second!.complete()
      await coordinator.close()
    })

    it('refuses a second lease while the first one is held', async () => {
      const coordinator = await create()
      const lease = await coordinator.acquire(key('exclusive'), { ttlMs: 500 })
      expect(lease).not.toBeNull()

      await expect(
        coordinator.acquire(key('exclusive'), { ttlMs: 500 })
      ).resolves.toBeNull()

      await lease!.complete()
      await coordinator.close()
    })

    it('renews a held lease and extends its expiry', async () => {
      const coordinator = await create()
      const lease = await coordinator.acquire(key('renew'), { ttlMs: 200 })
      expect(lease).not.toBeNull()

      await sleep(100)
      await expect(lease!.renew()).resolves.toBe(true)
      await sleep(150)

      // Past the original ttl, but the renewal moved the expiry out.
      await expect(
        coordinator.acquire(key('renew'), { ttlMs: 200 })
      ).resolves.toBeNull()

      await lease!.complete()
      await coordinator.close()
    })

    it('reports a lease past its ttl as no longer renewable', async () => {
      const coordinator = await create()
      const lease = await coordinator.acquire(key('expired'), { ttlMs: 30 })
      expect(lease).not.toBeNull()

      await sleep(60)

      await expect(lease!.renew()).resolves.toBe(false)
      await coordinator.close()
    })

    it('lets another owner take over after the ttl and ignores the stale owner', async () => {
      const coordinator = await create()
      const stale = await coordinator.acquire(key('stale'), { ttlMs: 30 })
      expect(stale).not.toBeNull()

      await sleep(60)

      const current = await coordinator.acquire(key('stale'), { ttlMs: 500 })
      expect(current).not.toBeNull()
      expect(current).not.toBe(stale)

      // A stale owner must not clear, extend, or renew the new owner's lease.
      await stale!.complete()
      await stale!.abandon()
      await expect(stale!.renew()).resolves.toBe(false)
      await expect(current!.renew()).resolves.toBe(true)

      await current!.complete()
      await coordinator.close()
    })

    it('returns from waitForChange when nothing changes', async () => {
      const coordinator = await create()
      const startedAt = Date.now()

      await expect(
        coordinator.waitForChange(key('wait-timeout'), { timeoutMs: 40 })
      ).resolves.toBeUndefined()

      expect(Date.now() - startedAt).toBeGreaterThanOrEqual(20)
      await coordinator.close()
    })

    it('rejects acquire and waitForChange for an already-aborted signal', async () => {
      const coordinator = await create()
      const controller = new AbortController()
      controller.abort(new Error('contract aborted'))

      await expect(
        coordinator.acquire(key('aborted'), {
          signal: controller.signal,
          ttlMs: 500,
        })
      ).rejects.toMatchObject({ name: 'AbortError' })

      await expect(
        coordinator.waitForChange(key('aborted'), {
          signal: controller.signal,
          timeoutMs: 40,
        })
      ).rejects.toMatchObject({ name: 'AbortError' })

      await coordinator.close()
    })

    it('closes idempotently', async () => {
      const coordinator = await create()

      await coordinator.close()
      await expect(coordinator.close()).resolves.toBeUndefined()
    })

    if (options.closesOperations) {
      it('rejects new work once closed', async () => {
        const coordinator = await create()
        await coordinator.close()

        await expect(
          coordinator.acquire(key('closed'), { ttlMs: 500 })
        ).rejects.toThrow(/clos/i)

        await expect(
          coordinator.waitForChange(key('closed'), { timeoutMs: 40 })
        ).rejects.toThrow(/clos/i)
      })
    }

    if (options.notifications) {
      it('wakes a waiter when the owner completes', async () => {
        const coordinator = await create()
        const waitKey = key('wake-complete')
        const waiter = coordinator.waitForChange(waitKey, { timeoutMs: 2000 })

        // Let the subscription settle before the change is signalled.
        await sleep(50)

        const lease = await coordinator.acquire(waitKey, { ttlMs: 2000 })
        expect(lease).not.toBeNull()

        const startedAt = Date.now()
        await lease!.complete()

        await expect(waiter).resolves.toBeUndefined()
        expect(Date.now() - startedAt).toBeLessThan(1000)
        await coordinator.close()
      })

      it('wakes a waiter when the owner abandons', async () => {
        const coordinator = await create()
        const waitKey = key('wake-abandon')
        const waiter = coordinator.waitForChange(waitKey, { timeoutMs: 2000 })

        await sleep(50)

        const lease = await coordinator.acquire(waitKey, { ttlMs: 2000 })
        expect(lease).not.toBeNull()

        const startedAt = Date.now()
        await lease!.abandon()

        await expect(waiter).resolves.toBeUndefined()
        expect(Date.now() - startedAt).toBeLessThan(1000)
        await coordinator.close()
      })

      it('wakes a waiter when the owner renews', async () => {
        const coordinator = await create()
        const waitKey = key('wake-renew')
        const waiter = coordinator.waitForChange(waitKey, { timeoutMs: 2000 })

        await sleep(50)

        const lease = await coordinator.acquire(waitKey, { ttlMs: 2000 })
        expect(lease).not.toBeNull()

        const startedAt = Date.now()
        await expect(lease!.renew()).resolves.toBe(true)

        await expect(waiter).resolves.toBeUndefined()
        expect(Date.now() - startedAt).toBeLessThan(1000)
        await coordinator.close()
      })

      it('wakes a waiter when another owner acquires the key', async () => {
        const coordinator = await create()
        const waitKey = key('wake-acquire')
        const waiter = coordinator.waitForChange(waitKey, { timeoutMs: 2000 })

        await sleep(50)

        const startedAt = Date.now()
        const lease = await coordinator.acquire(waitKey, { ttlMs: 2000 })
        expect(lease).not.toBeNull()

        await expect(waiter).resolves.toBeUndefined()
        expect(Date.now() - startedAt).toBeLessThan(1000)

        await lease!.complete()
        await coordinator.close()
      })
    }
  })
}
