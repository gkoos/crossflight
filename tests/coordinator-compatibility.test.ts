import { describe, expect, it } from 'vitest'

import { EventedMemoryCoordinator } from './mocks/evented-memory-coordinator.js'

describe('evented memory coordinator prototype', () => {
  it('acquires and renews a lease for the same key', async () => {
    const coordinator = new EventedMemoryCoordinator()

    const lease = await coordinator.acquire('compat:lease', { ttlMs: 100 })
    expect(lease).not.toBeNull()

    const renewed = await lease!.renew()
    expect(renewed).toBe(true)

    await lease!.complete()
    await coordinator.close()
  })

  it('releases a stale lease once its TTL expires', async () => {
    const coordinator = new EventedMemoryCoordinator()

    const first = await coordinator.acquire('compat:stale', { ttlMs: 20 })
    expect(first).not.toBeNull()

    await new Promise(resolve => setTimeout(resolve, 60))

    const second = await coordinator.acquire('compat:stale', { ttlMs: 60 })
    expect(second).not.toBeNull()
    expect(second).not.toBe(first)

    await second!.complete()
    await coordinator.close()
  })

  it('resolves waitForChange when a lease changes state', async () => {
    const coordinator = new EventedMemoryCoordinator()

    const waiter = coordinator.waitForChange('compat:wait', { timeoutMs: 100 })
    const lease = await coordinator.acquire('compat:wait', { ttlMs: 200 })
    expect(lease).not.toBeNull()

    await waiter
    await lease!.complete()
    await coordinator.close()
  })
})
