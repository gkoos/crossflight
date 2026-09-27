import { describe, expect, it, vi } from 'vitest'

import { raceWithAbort } from '../src/async-utils.js'
import {
  LeaseKeeper,
  MIN_LEASE_TTL_MS,
  MIN_RENEW_INTERVAL_MS,
} from '../src/lease-keeper.js'
import type { Lease } from '../src/types.js'

function makeLease(renew: () => Promise<boolean>): Lease {
  return {
    key: 'lease:keeper',
    renew,
    complete: async () => {},
    abandon: async () => {},
  }
}

function makeKeeper(options: {
  renew: () => Promise<boolean>
  renewIntervalMs?: number
  onOwnershipLost?: () => void
  onRenewError?: (error: unknown) => void
}): LeaseKeeper {
  const controller = new AbortController()
  return new LeaseKeeper({
    lease: makeLease(options.renew),
    renewIntervalMs: options.renewIntervalMs ?? 15,
    raceWithAbort,
    signal: controller.signal,
    onOwnershipLost: options.onOwnershipLost ?? (() => {}),
    onRenewError: options.onRenewError ?? (() => {}),
  })
}

const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

async function eventually(
  predicate: () => boolean,
  timeoutMs = 2000
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!predicate()) {
    if (Date.now() >= deadline) {
      throw new Error('condition not met within timeout')
    }
    await wait(10)
  }
}

describe('LeaseKeeper', () => {
  it('renews after the interval and chains the next renewal', async () => {
    const renew = vi.fn(async () => true)
    const keeper = makeKeeper({ renew, renewIntervalMs: 10 })
    keeper.start()

    await eventually(() => renew.mock.calls.length >= 1)
    await eventually(() => renew.mock.calls.length >= 2)

    await keeper.stop()
  })

  it('does not start the next renewal until the current one settles', async () => {
    const resolvers: Array<(value: boolean) => void> = []
    const renew = vi.fn(
      () => new Promise<boolean>((resolve) => resolvers.push(resolve))
    )
    const keeper = makeKeeper({ renew, renewIntervalMs: 10 })
    keeper.start()

    await eventually(() => renew.mock.calls.length >= 1)

    await wait(50)
    expect(renew).toHaveBeenCalledTimes(1)

    resolvers[0]!(true)
    await eventually(() => renew.mock.calls.length >= 2)

    resolvers[1]!(true)
    await keeper.stop()
  })

  it('reports ownership loss when renew returns false and stops scheduling', async () => {
    const onOwnershipLost = vi.fn()
    const renew = vi.fn(async () => false)
    const keeper = makeKeeper({ renew, onOwnershipLost })
    keeper.start()

    await wait(30)
    expect(onOwnershipLost).toHaveBeenCalledTimes(1)

    await wait(60)
    expect(renew).toHaveBeenCalledTimes(1)
  })

  it('reports a renewal error and stops scheduling', async () => {
    const onRenewError = vi.fn()
    const boom = new Error('boom')
    const renew = vi.fn(async () => {
      throw boom
    })
    const keeper = makeKeeper({ renew, onRenewError })
    keeper.start()

    await wait(30)
    expect(onRenewError).toHaveBeenCalledWith(boom)

    await wait(60)
    expect(renew).toHaveBeenCalledTimes(1)
  })

  it('stop clears a pending timer without renewing', async () => {
    const renew = vi.fn(async () => true)
    const keeper = makeKeeper({ renew })
    keeper.start()
    await keeper.stop()

    await wait(60)
    expect(renew).not.toHaveBeenCalled()
  })

  it('stop drains an in-flight renewal', async () => {
    let resolveRenew: (value: boolean) => void = () => {}
    const renew = vi.fn(
      () => new Promise<boolean>((resolve) => (resolveRenew = resolve))
    )
    const keeper = makeKeeper({ renew, renewIntervalMs: 10 })
    keeper.start()

    await wait(30)
    expect(renew).toHaveBeenCalledTimes(1)

    const stopPromise = keeper.stop()
    resolveRenew(true)
    await stopPromise

    await wait(60)
    expect(renew).toHaveBeenCalledTimes(1)
  })

  it('stop is idempotent', async () => {
    const renew = vi.fn(async () => true)
    const keeper = makeKeeper({ renew })
    keeper.start()
    await keeper.stop()
    await expect(keeper.stop()).resolves.toBeUndefined()
  })

  it('start after stop schedules nothing', async () => {
    const renew = vi.fn(async () => true)
    const keeper = makeKeeper({ renew })
    keeper.start()
    await keeper.stop()
    keeper.start()

    await wait(60)
    expect(renew).not.toHaveBeenCalled()
  })
})

describe('lease timing constants', () => {
  it('keeps the minimum lease ttl at twice the renewal interval', () => {
    expect(MIN_LEASE_TTL_MS).toBe(2 * MIN_RENEW_INTERVAL_MS)
  })
})
