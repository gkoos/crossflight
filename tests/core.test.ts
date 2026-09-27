import { spawnSync } from 'node:child_process'
import { getEventListeners } from 'node:events'

import { describe, expect, it, vi } from 'vitest'

import {
  createCrossflight,
  CoordinationError,
  CoordinationClosedError,
  CoordinationTimeoutError,
  OwnershipLostError,
} from '../src/index.js'
import { InMemoryCoordinator } from './mocks/in-memory-coordinator.js'

class MemoryCache {
  private readonly values = new Map<string, unknown>()

  async get<T>(key: string) {
    if (this.values.has(key)) {
      return { hit: true as const, value: this.values.get(key) as T }
    }

    return { hit: false as const }
  }

  async set<T>(key: string, value: T) {
    this.values.set(key, value)
  }
}

describe('crossflight core', () => {
  it('coalesces concurrent same-process misses into a single loader run', async () => {
    const cache = new MemoryCache()
    const coordinator = new InMemoryCoordinator()
    const crossflight = createCrossflight({ cache, coordinator })

    let loadRuns = 0

    const loader = async () => {
      loadRuns += 1
      await new Promise(resolve => setTimeout(resolve, 25))
      return 'computed-value'
    }

    const results = await Promise.all([
      crossflight.wrap('user:123', loader),
      crossflight.wrap('user:123', loader),
    ])

    expect(results).toEqual(['computed-value', 'computed-value'])
    expect(loadRuns).toBe(1)
    await crossflight.close()
  })

  it('returns the cached value without invoking the loader again', async () => {
    const cache = new MemoryCache()
    const coordinator = new InMemoryCoordinator()
    const crossflight = createCrossflight({ cache, coordinator })

    await cache.set('cached:1', 'cached-value')

    const result = await crossflight.wrap('cached:1', async () => 'should-not-run')

    expect(result).toBe('cached-value')
    await crossflight.close()
  })

  it('extends the in-memory lease ttl when it is renewed', async () => {
    const coordinator = new InMemoryCoordinator()
    const lease = await coordinator.acquire('lease:ttl', { ttlMs: 90 })

    expect(lease).not.toBeNull()

    await new Promise(resolve => setTimeout(resolve, 50))
    expect(await lease!.renew()).toBe(true)

    await new Promise(resolve => setTimeout(resolve, 40))
    expect(await lease!.renew()).toBe(true)

    await coordinator.close()
  })

  it('renews ownership periodically while a long-running loader is active', async () => {
    const cache = new MemoryCache()
    let renewCalls = 0

    const coordinator = {
      async acquire(key: string) {
        return {
          key,
          async renew() {
            renewCalls += 1
            return true
          },
          async complete() {},
          async abandon() {},
        }
      },
      async waitForChange() {},
      async close() {},
    }

    const crossflight = createCrossflight({ cache, coordinator })

    await expect(
      crossflight.wrap('renew:periodic:key', async () => {
        await new Promise(resolve => setTimeout(resolve, 120))
        return 'value'
      }, { ttl: 40 })
    ).resolves.toBe('value')

    expect(renewCalls).toBeGreaterThanOrEqual(2)
    await crossflight.close()
  })

  it('attempts lease acquisition again after waking up to a cache miss', async () => {
    const cache = new MemoryCache()
    let acquireCalls = 0
    let waitCalls = 0

    const coordinator = {
      async acquire(key: string) {
        acquireCalls += 1
        if (acquireCalls === 1) {
          return null
        }

        return {
          key,
          async renew() { return true },
          async complete() {},
          async abandon() {},
        }
      },
      async waitForChange() {
        waitCalls += 1
      },
      async close() {},
    }

    const crossflight = createCrossflight({ cache, coordinator, maxRetryAttempts: 2 })

    await expect(
      crossflight.wrap('waiter:reacquire:key', async () => 'owned-after-reacquire')
    ).resolves.toBe('owned-after-reacquire')

    expect(waitCalls).toBe(1)
    expect(acquireCalls).toBe(2)
    await crossflight.close()
  })

  it('aborts an in-memory wait when its signal is cancelled', async () => {
    const coordinator = new InMemoryCoordinator()
    const controller = new AbortController()

    const wait = coordinator.waitForChange('wait:abort', {
      signal: controller.signal,
      timeoutMs: 500,
    })

    controller.abort()

    await expect(wait).rejects.toThrow(/aborted|AbortError|aborted/i)

    await coordinator.close()
  })

  it('rejects stale lease operations against a newer owner', async () => {
    const coordinator = new InMemoryCoordinator()

    const firstLease = await coordinator.acquire('stale:lease', { ttlMs: 40 })
    expect(firstLease).not.toBeNull()

    await new Promise(resolve => setTimeout(resolve, 60))

    const secondLease = await coordinator.acquire('stale:lease', { ttlMs: 200 })
    expect(secondLease).not.toBeNull()

    await firstLease!.complete()
    expect(await secondLease!.renew()).toBe(true)

    const current = coordinator.owners.get('stale:lease')
    expect(current).toBeDefined()
    expect(current?.expiresAt).toBeGreaterThan(Date.now())

    await secondLease!.complete()
    await coordinator.close()
  })

  it('does not cache a value when the loader throws', async () => {
    const cache = new MemoryCache()
    const coordinator = new InMemoryCoordinator()
    const crossflight = createCrossflight({ cache, coordinator })

    await expect(
      crossflight.wrap('load:fail', async () => {
        throw new Error('boom')
      })
    ).rejects.toThrow('boom')

    expect(await cache.get('load:fail')).toEqual({ hit: false })
    await crossflight.close()
  })

  it('abandons ownership if cache.set fails before completion', async () => {
    const cache = new MemoryCache()
    const coordinator = new InMemoryCoordinator()
    const crossflight = createCrossflight({ cache, coordinator })

    const originalSet = cache.set.bind(cache)
    cache.set = async () => {
      throw new Error('cache write failed')
    }

    await expect(
      crossflight.wrap('write:fail', async () => 'value')
    ).rejects.toThrow('cache write failed')

    const current = coordinator.owners.get('write:fail')
    expect(current).toBeUndefined()

    cache.set = originalSet
    await crossflight.close()
  })

  it('eventually resolves when a distributed owner takes longer than the retry window', async () => {
    const cache = new MemoryCache()
    let ownerCreated = false
    let ownerDone = false

    const coordinator = {
      async acquire(key: string) {
        if (key !== 'slow:distributed:key') {
          return null
        }

        if (ownerCreated) {
          return null
        }

        ownerCreated = true
        return {
          key,
          async renew() {
            return true
          },
          async complete() {
            ownerDone = true
            return undefined
          },
          async abandon() {
            return undefined
          },
        }
      },
      async waitForChange() {
        await new Promise(resolve => setTimeout(resolve, 50))
      },
      async close() {},
    }

    const crossflight = createCrossflight({ cache, coordinator })

    const winner = crossflight.wrap('slow:distributed:key', async () => {
      await new Promise(resolve => setTimeout(resolve, 1500))
      await cache.set('slow:distributed:key', 'value')
      ownerDone = true
      return 'value'
    })

    const loser = crossflight.wrap('slow:distributed:key', async () => 'should-not-run')

    await expect(Promise.race([
      Promise.all([winner, loser]),
      new Promise((_, reject) => setTimeout(() => reject(new Error('timed out waiting for distributed load')), 5000)),
    ])).resolves.toEqual(['value', 'value'])

    expect(ownerDone).toBe(true)
    await crossflight.close()
  })

  it('fails closed when coordination is unavailable', async () => {
    const cache = new MemoryCache()
    const coordinator = {
      async acquire() {
        throw new Error('coordinator unavailable')
      },
      async waitForChange() {
        throw new Error('coordinator unavailable')
      },
      async close() {},
    }

    const crossflight = createCrossflight({
      cache,
      coordinator,
      failureMode: 'fail-closed',
    })

    await expect(
      crossflight.wrap('coordination:down', async () => 'fallback')
    ).rejects.toThrow('coordinator unavailable')

    await crossflight.close()
  })

  it('fails open when coordination is unavailable and falls back to the loader', async () => {
    const cache = new MemoryCache()
    const coordinator = {
      async acquire() {
        throw new Error('coordinator unavailable')
      },
      async waitForChange() {
        throw new Error('coordinator unavailable')
      },
      async close() {},
    }

    const crossflight = createCrossflight({
      cache,
      coordinator,
      failureMode: 'fail-open',
    })

    await expect(
      crossflight.wrap('coordination:recovery', async () => 'fallback-value')
    ).resolves.toBe('fallback-value')

    await crossflight.close()
  })

  it('enforces a per-call timeout override', async () => {
    const cache = new MemoryCache()
    const coordinator = new InMemoryCoordinator()
    const crossflight = createCrossflight({
      cache,
      coordinator,
      defaultTimeoutMs: 50,
    })

    const err = await crossflight
      .wrap(
        'timeout:override',
        async () => {
          await new Promise(resolve => setTimeout(resolve, 200))
          return 'too-late'
        },
        { timeoutMs: 20 }
      )
      .catch(e => e)

    expect(err).toBeInstanceOf(CoordinationTimeoutError)
    expect((err as CoordinationTimeoutError).key).toBe('timeout:override')

    await crossflight.close()
  })

  it('aborts in-flight work when close is called', async () => {
    const cache = new MemoryCache()
    const coordinator = new InMemoryCoordinator()
    const crossflight = createCrossflight({ cache, coordinator })

    const pending = crossflight.wrap('close:abort', async () => {
      await new Promise(resolve => setTimeout(resolve, 200))
      return 'done'
    })

    await crossflight.close()

    await expect(pending).rejects.toThrow(/aborted|close|timeout/i)
  })

  it('throws CoordinationClosedError when close() is called mid-flight', async () => {
    const cache = new MemoryCache()
    const coordinator = new InMemoryCoordinator()
    const crossflight = createCrossflight({ cache, coordinator })

    const pending = crossflight.wrap('close:error:type', async () => {
      await new Promise(resolve => setTimeout(resolve, 200))
      return 'done'
    })

    await crossflight.close()

    await expect(pending).rejects.toBeInstanceOf(CoordinationClosedError)
    await expect(pending).rejects.toBeInstanceOf(CoordinationError)
  })

  it('throws CoordinationTimeoutError after exhausting distributed retries', async () => {
    const cache = new MemoryCache()
    const coordinator = {
      async acquire() {
        return null // always someone else owns it
      },
      async waitForChange() {
        // return immediately so retries burn through fast
      },
      async close() {},
    }

    const crossflight = createCrossflight({ cache, coordinator })

    const error = await crossflight
      .wrap('timeout:error:type', async () => 'never')
      .catch(e => e)

    expect(error).toBeInstanceOf(CoordinationTimeoutError)
    expect(error).toBeInstanceOf(CoordinationError)
    expect((error as CoordinationTimeoutError).key).toBe('timeout:error:type')
    await crossflight.close()
  })

  it('emits OwnershipLostError via onEvent when lease renewal fails', async () => {
    const cache = new MemoryCache()
    let leasePrepared = false
    const failRenew = { shouldFail: false }

    const coordinator = {
      async acquire(key: string) {
        if (leasePrepared) return null
        leasePrepared = true
        return {
          key,
          async renew() {
            if (failRenew.shouldFail) return false
            return true
          },
          async complete() {},
          async abandon() {},
        }
      },
      async waitForChange() {},
      async close() {},
    }

    const events: unknown[] = []
    const crossflight = createCrossflight({
      cache,
      coordinator,
      onEvent: e => events.push(e),
    })

    failRenew.shouldFail = true

    // After ownership is lost, crossflight retries. The retry sees null from acquire
    // (leasePrepared=true) and waits via waitForChange until the cache is populated.
    // We need to populate the cache during the retry window.
    let retries = 0
    coordinator.waitForChange = async () => {
      retries += 1
      if (retries === 1) {
        await cache.set('ownership:lost:key', 'recovered-value')
      }
    }

    const result = await crossflight.wrap('ownership:lost:key', async () => 'original')

    const lostEvent = events.find(
      e => (e as { type: string }).type === 'failed' &&
           (e as { error: unknown }).error instanceof OwnershipLostError
    ) as { error: OwnershipLostError } | undefined

    expect(lostEvent).toBeDefined()
    expect(lostEvent!.error).toBeInstanceOf(OwnershipLostError)
    expect(lostEvent!.error).toBeInstanceOf(CoordinationError)
    expect(lostEvent!.error.key).toBe('ownership:lost:key')
    expect(result).toBe('recovered-value')

    await crossflight.close()
  })

  it('emits a minimal event stream for cache hits and completed loads', async () => {
    const cache = new MemoryCache()
    const coordinator = new InMemoryCoordinator()
    const events: Array<{ type: string; key: string; durationMs?: number }> = []
    const crossflight = createCrossflight({
      cache,
      coordinator,
      onEvent: event => {
        events.push(event as { type: string; key: string; durationMs?: number })
      },
    })

    await crossflight.wrap('events:key', async () => 'value')
    await crossflight.wrap('events:key', async () => 'should-not-run')

    expect(events.some(event => event.type === 'miss' && event.key === 'events:key')).toBe(true)
    expect(events.some(event => event.type === 'ownership_acquired' && event.key === 'events:key')).toBe(true)
    expect(events.some(event => event.type === 'completed' && event.key === 'events:key')).toBe(true)
    expect(events.some(event => event.type === 'hit' && event.key === 'events:key')).toBe(true)

    await crossflight.close()
  })

  it('respects defaultTtlMs when wrap() caller does not specify ttl', async () => {
    const cache = new MemoryCache()
    let capturedTtlMs: number | undefined

    const coordinator = {
      async acquire(_key: string, options?: { ttlMs?: number }) {
        capturedTtlMs = options?.ttlMs
        return null
      },
      async waitForChange() {},
      async close() {},
    }

    const crossflight = createCrossflight({
      cache,
      coordinator,
      defaultTtlMs: 5_000,
      failureMode: 'fail-open',
    })

    await crossflight.wrap('ttl:key', async () => 'value')
    expect(capturedTtlMs).toBe(5_000)
    await crossflight.close()
  })

  it('respects maxRetryAttempts before throwing CoordinationTimeoutError', async () => {
    const cache = new MemoryCache()
    let waitCalls = 0

    const coordinator = {
      async acquire() { return null },
      async waitForChange() { waitCalls += 1 },
      async close() {},
    }

    const crossflight = createCrossflight({ cache, coordinator, maxRetryAttempts: 3 })

    await expect(
      crossflight.wrap('retry:key', async () => 'never')
    ).rejects.toBeInstanceOf(CoordinationTimeoutError)

    expect(waitCalls).toBe(3)
    await crossflight.close()
  })

  it('uses retryBackoff to determine wait delay per attempt', async () => {
    const cache = new MemoryCache()
    const capturedAttempts: number[] = []

    const coordinator = {
      async acquire() { return null },
      async waitForChange(_key: string, options?: { timeoutMs?: number }) {
        capturedAttempts.push(options?.timeoutMs ?? -1)
      },
      async close() {},
    }

    const crossflight = createCrossflight({
      cache,
      coordinator,
      maxRetryAttempts: 3,
      retryBackoff: attempt => (attempt + 1) * 10,
    })

    await expect(
      crossflight.wrap('backoff:key', async () => 'never')
    ).rejects.toBeInstanceOf(CoordinationTimeoutError)

    expect(capturedAttempts).toEqual([10, 20, 30])
    await crossflight.close()
  })

  it('calls onEventError when onEvent throws', async () => {
    const cache = new MemoryCache()
    const coordinator = new InMemoryCoordinator()
    const eventErrors: unknown[] = []

    const crossflight = createCrossflight({
      cache,
      coordinator,
      onEvent: () => {
        throw new Error('observer boom')
      },
      onEventError: error => eventErrors.push(error),
    })

    await crossflight.wrap('event:error:key', async () => 'value')

    expect(eventErrors.length).toBeGreaterThan(0)
    expect((eventErrors[0] as Error).message).toBe('observer boom')
    await crossflight.close()
  })

  it('does not throw when onEvent throws and onEventError is not set', async () => {
    const cache = new MemoryCache()
    const coordinator = new InMemoryCoordinator()

    const crossflight = createCrossflight({
      cache,
      coordinator,
      onEvent: () => {
        throw new Error('observer boom')
      },
    })

    await expect(
      crossflight.wrap('event:silent:key', async () => 'value')
    ).resolves.toBe('value')

    await crossflight.close()
  })

  it('aborts immediately when the caller signal is already aborted before wrap()', async () => {
    const cache = new MemoryCache()
    const coordinator = new InMemoryCoordinator()
    const crossflight = createCrossflight({ cache, coordinator })

    const controller = new AbortController()
    controller.abort(new Error('pre-aborted'))

    await expect(
      crossflight.wrap('pre:aborted:key', async () => 'value', { signal: controller.signal })
    ).rejects.toThrow('pre-aborted')

    await crossflight.close()
  })

  it('returns cached value found during ownership recheck and releases the lease', async () => {
    const cache = new MemoryCache()
    let leaseAcquired = false
    let loaderRan = false
    let abandonCalls = 0
    let completeCalls = 0

    const coordinator = {
      async acquire(key: string) {
        if (leaseAcquired) return null
        leaseAcquired = true
        // Populate cache between acquire and loader so recheck hits
        await cache.set(key, 'populated-between')
        return {
          key,
          async renew() { return true },
          async complete() { completeCalls += 1 },
          async abandon() { abandonCalls += 1 },
        }
      },
      async waitForChange() {},
      async close() {},
    }

    const crossflight = createCrossflight({ cache, coordinator })

    const result = await crossflight.wrap('recheck:hit:key', async () => {
      loaderRan = true
      return 'from-loader'
    })

    expect(result).toBe('populated-between')
    expect(loaderRan).toBe(false)
    // The lease protects nothing once another owner cached the value.
    expect(abandonCalls).toBe(1)
    expect(completeCalls).toBe(0)
    await crossflight.close()
  })

  it('does not retain ownership when the recheck finds a cached value', async () => {
    const cache = new MemoryCache()
    const coordinator = new InMemoryCoordinator()
    const originalAcquire = coordinator.acquire.bind(coordinator)

    // Fill the cache between acquiring ownership and the recheck.
    coordinator.acquire = async (key: string) => {
      const lease = await originalAcquire(key)
      if (lease) {
        await cache.set(key, 'populated-between')
      }

      return lease
    }

    const crossflight = createCrossflight({ cache, coordinator })
    let loaderRan = false

    const result = await crossflight.wrap('recheck:release:key', async () => {
      loaderRan = true
      return 'from-loader'
    })

    expect(result).toBe('populated-between')
    expect(loaderRan).toBe(false)
    expect(coordinator.owners.get('recheck:release:key')).toBeUndefined()
    await crossflight.close()
  })

  it('falls back to loader when waitForChange throws and failureMode is fail-open', async () => {
    const cache = new MemoryCache()
    const coordinator = {
      async acquire() { return null },
      async waitForChange() { throw new Error('wait boom') },
      async close() {},
    }

    const crossflight = createCrossflight({ cache, coordinator, failureMode: 'fail-open' })

    await expect(
      crossflight.wrap('wait:fail:open:key', async () => 'fallback')
    ).resolves.toBe('fallback')

    await crossflight.close()
  })

  it('falls back to loader when waiter wakes to a miss and still cannot acquire ownership in fail-open mode', async () => {
    const cache = new MemoryCache()
    let acquireCalls = 0

    const coordinator = {
      async acquire() {
        acquireCalls += 1
        return null
      },
      async waitForChange() {},
      async close() {},
    }

    const crossflight = createCrossflight({
      cache,
      coordinator,
      failureMode: 'fail-open',
      maxRetryAttempts: 1,
    })

    await expect(
      crossflight.wrap('waiter:miss:fail-open:key', async () => 'fallback-after-miss')
    ).resolves.toBe('fallback-after-miss')

    expect(acquireCalls).toBe(2)
    await crossflight.close()
  })

  it('falls back to loader when acquire throws and failureMode is fail-open', async () => {
    const cache = new MemoryCache()
    const coordinator = {
      async acquire() {
        throw new Error('acquire boom')
      },
      async waitForChange() {},
      async close() {},
    }

    const crossflight = createCrossflight({ cache, coordinator, failureMode: 'fail-open' })

    await expect(
      crossflight.wrap('acquire:fail:open:key', async () => 'fallback-acquire')
    ).resolves.toBe('fallback-acquire')

    await crossflight.close()
  })

  it('does not enter the distributed waiter loop when the acquire itself fails in fail-open mode', async () => {
    const cache = new MemoryCache()
    let waitCalls = 0
    const events: Array<{ type: string }> = []

    const coordinator = {
      async acquire() {
        throw new Error('acquire boom')
      },
      async waitForChange() {
        waitCalls += 1
      },
      async close() {},
    }

    const crossflight = createCrossflight({
      cache,
      coordinator,
      failureMode: 'fail-open',
      onEvent: event => events.push(event as { type: string }),
    })

    await expect(
      crossflight.wrap('fail:open:no:waiter', async () => 'fallback')
    ).resolves.toBe('fallback')

    // A failed acquire short-circuits to the loader, so the waiter loop is not
    // entered; only ordinary contention reaches it in fail-open mode.
    expect(waitCalls).toBe(0)
    expect(events.some(event => event.type === 'distributed_join')).toBe(false)

    await crossflight.close()
  })

  it('joins the distributed wait instead of loading when the lease is contended in fail-open mode', async () => {
    const cache = new MemoryCache()
    const coordinator = new InMemoryCoordinator()
    const events: Array<{ type: string }> = []
    let loadRuns = 0
    let releaseOwner: () => void = () => {}
    const ownerGate = new Promise<void>(resolve => {
      releaseOwner = resolve
    })
    let ownerStarted: () => void = () => {}
    const ownerStartedGate = new Promise<void>(resolve => {
      ownerStarted = resolve
    })

    const owner = createCrossflight({ cache, coordinator })
    const fallback = createCrossflight({
      cache,
      coordinator,
      failureMode: 'fail-open',
      onEvent: event => events.push(event as { type: string }),
    })

    const ownerResult = owner.wrap('fail:open:contended:key', async () => {
      loadRuns += 1
      ownerStarted()
      await ownerGate
      return 'owner-value'
    })

    // The loader runs only once the owner holds the lease, so this is the
    // deterministic point at which the key is contended.
    await ownerStartedGate

    const fallbackResult = fallback.wrap('fail:open:contended:key', async () => {
      loadRuns += 1
      return 'fallback-value'
    })

    releaseOwner()

    await expect(ownerResult).resolves.toBe('owner-value')
    await expect(fallbackResult).resolves.toBe('owner-value')
    expect(loadRuns).toBe(1)
    expect(events.some(event => event.type === 'distributed_join')).toBe(true)
    expect(events.some(event => event.type === 'hit')).toBe(true)

    await owner.close()
    await fallback.close()
  })

  it('becomes the owner in fail-open mode when a contended lease is released without a value', async () => {
    const cache = new MemoryCache()
    let acquireCalls = 0

    const coordinator = {
      async acquire(key: string) {
        acquireCalls += 1
        if (acquireCalls === 1) {
          return null
        }

        return {
          key,
          async renew() {
            return true
          },
          async complete() {},
          async abandon() {},
        }
      },
      async waitForChange() {},
      async close() {},
    }

    const crossflight = createCrossflight({
      cache,
      coordinator,
      failureMode: 'fail-open',
      maxRetryAttempts: 2,
    })

    await expect(
      crossflight.wrap('fail:open:reacquire:key', async () => 'own-value')
    ).resolves.toBe('own-value')

    expect(acquireCalls).toBe(2)
    await crossflight.close()
  })

  it('falls back to loader when a contended re-acquire fails in fail-open mode', async () => {
    const cache = new MemoryCache()
    let acquireCalls = 0
    const events: Array<{ type: string; error?: unknown }> = []

    const coordinator = {
      async acquire() {
        acquireCalls += 1
        if (acquireCalls === 1) {
          return null
        }
        throw new Error('acquire boom')
      },
      async waitForChange() {},
      async close() {},
    }

    const crossflight = createCrossflight({
      cache,
      coordinator,
      failureMode: 'fail-open',
      maxRetryAttempts: 2,
      onEvent: event => events.push(event as { type: string; error?: unknown }),
    })

    await expect(
      crossflight.wrap('fail:open:reacquire:fail', async () => 'fallback-value')
    ).resolves.toBe('fallback-value')

    expect(acquireCalls).toBe(2)
    const failed = events.filter(event => event.type === 'failed')
    expect(failed).toHaveLength(1)
    expect((failed[0]!.error as Error).message).toBe('acquire boom')

    await crossflight.close()
  })

  it('fails with the renewal error when periodic renewal throws during owner execution', async () => {
    const cache = new MemoryCache()
    let renewCalls = 0

    const coordinator = {
      async acquire(key: string) {
        return {
          key,
          async renew() {
            renewCalls += 1
            if (renewCalls >= 2) {
              throw new Error('renew failed')
            }
            return true
          },
          async complete() {},
          async abandon() {},
        }
      },
      async waitForChange() {},
      async close() {},
    }

    const crossflight = createCrossflight({ cache, coordinator })

    await expect(
      crossflight.wrap('renew:error:key', async () => {
        await new Promise(resolve => setTimeout(resolve, 120))
        return 'value'
      }, { ttl: 40 })
    ).rejects.toThrow('renew failed')

    await crossflight.close()
  })

  it('emits renewal_failed event when periodic renewal throws during owner execution', async () => {
    const cache = new MemoryCache()
    let renewCalls = 0

    const coordinator = {
      async acquire(key: string) {
        return {
          key,
          async renew() {
            renewCalls += 1
            if (renewCalls >= 2) {
              throw new Error('renew blip')
            }
            return true
          },
          async complete() {},
          async abandon() {},
        }
      },
      async waitForChange() {},
      async close() {},
    }

    const events: unknown[] = []
    const crossflight = createCrossflight({
      cache,
      coordinator,
      onEvent: e => events.push(e),
    })

    await expect(
      crossflight.wrap('renew:event:key', async () => {
        await new Promise(resolve => setTimeout(resolve, 120))
        return 'value'
      }, { ttl: 40 })
    ).rejects.toThrow('renew blip')

    const renewalFailedEvent = events.find(
      e => (e as { type: string }).type === 'renewal_failed'
    ) as { type: string; key: string; error: unknown } | undefined

    expect(renewalFailedEvent).toBeDefined()
    expect(renewalFailedEvent!.key).toBe('renew:event:key')
    expect((renewalFailedEvent!.error as Error).message).toBe('renew blip')

    await crossflight.close()
  })

  it('swallows errors thrown by onEventError itself', async () => {
    const cache = new MemoryCache()
    const coordinator = new InMemoryCoordinator()

    const crossflight = createCrossflight({
      cache,
      coordinator,
      onEvent: () => { throw new Error('observer boom') },
      onEventError: () => { throw new Error('error handler also boom') },
    })

    await expect(
      crossflight.wrap('event:error:handler:throws', async () => 'value')
    ).resolves.toBe('value')

    await crossflight.close()
  })

  describe('failed event deduplication', () => {
    type ObservedEvent = { type: string; key?: string; error?: unknown }

    const failedEvents = (events: ObservedEvent[]) =>
      events.filter(event => event.type === 'failed')

    const failingCache = () => {
      const cache = new MemoryCache()
      cache.get = async () => {
        throw new Error('cache get boom')
      }
      return cache
    }

    const throwOnWaitCoordinator = (message: string) => ({
      async acquire() {
        return null
      },
      async waitForChange(): Promise<never> {
        throw new Error(message)
      },
      async close() {},
    })

    it('emits exactly one failed event when acquire throws', async () => {
      const events: ObservedEvent[] = []
      const coordinator = {
        async acquire(): Promise<never> {
          throw new Error('acquire boom')
        },
        async waitForChange() {},
        async close() {},
      }
      const crossflight = createCrossflight({
        cache: new MemoryCache(),
        coordinator,
        onEvent: event => events.push(event as ObservedEvent),
      })

      await expect(
        crossflight.wrap('dedupe:acquire:throw', async () => 'value')
      ).rejects.toThrow('acquire boom')

      const failed = failedEvents(events)
      expect(failed).toHaveLength(1)
      expect((failed[0]!.error as Error).message).toBe('acquire boom')

      await crossflight.close()
    })

    it('emits exactly one failed event when waitForChange throws', async () => {
      const events: ObservedEvent[] = []
      const crossflight = createCrossflight({
        cache: new MemoryCache(),
        coordinator: throwOnWaitCoordinator('wait boom'),
        maxRetryAttempts: 2,
        onEvent: event => events.push(event as ObservedEvent),
      })

      await expect(
        crossflight.wrap('dedupe:wait:throw', async () => 'value')
      ).rejects.toThrow('wait boom')

      const failed = failedEvents(events)
      expect(failed).toHaveLength(1)
      expect((failed[0]!.error as Error).message).toBe('wait boom')

      await crossflight.close()
    })

    it('emits exactly one failed event after retry exhaustion', async () => {
      const events: ObservedEvent[] = []
      const coordinator = {
        async acquire() {
          return null
        },
        async waitForChange() {},
        async close() {},
      }
      const crossflight = createCrossflight({
        cache: new MemoryCache(),
        coordinator,
        maxRetryAttempts: 1,
        onEvent: event => events.push(event as ObservedEvent),
      })

      await expect(
        crossflight.wrap('dedupe:retry:exhausted', async () => 'value')
      ).rejects.toBeInstanceOf(CoordinationTimeoutError)

      const failed = failedEvents(events)
      expect(failed).toHaveLength(1)
      expect(failed[0]!.error).toBeInstanceOf(CoordinationTimeoutError)

      await crossflight.close()
    })

    it('emits exactly one failed event when the owner loader throws', async () => {
      const events: ObservedEvent[] = []
      const coordinator = {
        async acquire(key: string) {
          return {
            key,
            async renew() { return true },
            async complete() {},
            async abandon() {},
          }
        },
        async waitForChange() {},
        async close() {},
      }
      const crossflight = createCrossflight({
        cache: new MemoryCache(),
        coordinator,
        onEvent: event => events.push(event as ObservedEvent),
      })

      await expect(
        crossflight.wrap('dedupe:loader:throw', async () => {
          throw new Error('loader boom')
        })
      ).rejects.toThrow('loader boom')

      const failed = failedEvents(events)
      expect(failed).toHaveLength(1)
      expect((failed[0]!.error as Error).message).toBe('loader boom')

      await crossflight.close()
    })

    // Regression guard: this failure is only observed by the outer catch, so
    // removing that emit (as originally proposed) would drop the event entirely.
    it('emits exactly one failed event when the initial cache read throws', async () => {
      const events: ObservedEvent[] = []
      const crossflight = createCrossflight({
        cache: failingCache(),
        coordinator: new InMemoryCoordinator(),
        onEvent: event => events.push(event as ObservedEvent),
      })

      await expect(
        crossflight.wrap('dedupe:cache:get:throw', async () => 'value')
      ).rejects.toThrow('cache get boom')

      const failed = failedEvents(events)
      expect(failed).toHaveLength(1)
      expect((failed[0]!.error as Error).message).toBe('cache get boom')

      await crossflight.close()
    })

    // Regression guard: the exhausted contention and the fallback loader error
    // are each reported exactly once.
    it('reports an exhausted contended wait and the fail-open fallback loader error once each', async () => {
      const events: ObservedEvent[] = []
      const coordinator = {
        async acquire() {
          return null
        },
        async waitForChange() {},
        async close() {},
      }
      const crossflight = createCrossflight({
        cache: new MemoryCache(),
        coordinator,
        failureMode: 'fail-open',
        maxRetryAttempts: 1,
        onEvent: event => events.push(event as ObservedEvent),
      })

      await expect(
        crossflight.wrap('dedupe:fail:open:loader:throw', async () => {
          throw new Error('fallback boom')
        })
      ).rejects.toThrow('fallback boom')

      const failed = failedEvents(events)
      expect(failed).toHaveLength(2)
      expect(failed[0]!.error).toBeInstanceOf(CoordinationTimeoutError)
      expect((failed[1]!.error as Error).message).toBe('fallback boom')

      await crossflight.close()
    })


    it('emits exactly one failed event when cache.set throws', async () => {
      const events: ObservedEvent[] = []
      const cache = new MemoryCache()
      cache.set = async () => {
        throw new Error('cache set boom')
      }
      const coordinator = {
        async acquire(key: string) {
          return {
            key,
            async renew() { return true },
            async complete() {},
            async abandon() {},
          }
        },
        async waitForChange() {},
        async close() {},
      }
      const crossflight = createCrossflight({
        cache,
        coordinator,
        onEvent: event => events.push(event as ObservedEvent),
      })

      await expect(
        crossflight.wrap('dedupe:cache:set:throw', async () => 'value')
      ).rejects.toThrow('cache set boom')

      const failed = failedEvents(events)
      expect(failed).toHaveLength(1)
      expect((failed[0]!.error as Error).message).toBe('cache set boom')

      await crossflight.close()
    })

    it('emits exactly one failed event when periodic renewal throws', async () => {
      const events: ObservedEvent[] = []
      let renewCalls = 0
      const coordinator = {
        async acquire(key: string) {
          return {
            key,
            async renew() {
              renewCalls += 1
              if (renewCalls >= 2) {
                throw new Error('renew boom')
              }
              return true
            },
            async complete() {},
            async abandon() {},
          }
        },
        async waitForChange() {},
        async close() {},
      }
      const crossflight = createCrossflight({
        cache: new MemoryCache(),
        coordinator,
        onEvent: event => events.push(event as ObservedEvent),
      })

      await expect(
        crossflight.wrap(
          'dedupe:renew:throw',
          async () => {
            await new Promise(resolve => setTimeout(resolve, 120))
            return 'value'
          },
          { ttl: 40 }
        )
      ).rejects.toThrow('renew boom')

      const failed = failedEvents(events)
      const renewalFailed = events.filter(event => event.type === 'renewal_failed')
      expect(renewalFailed).toHaveLength(1)
      expect(failed).toHaveLength(1)
      expect((failed[0]!.error as Error).message).toBe('renew boom')

      await crossflight.close()
    })

    it('emits exactly one failed event, the timeout, when a waiter is aborted', async () => {
      const events: ObservedEvent[] = []
      const coordinator = {
        async acquire() {
          return null
        },
        async waitForChange(_key: string, options?: { signal?: AbortSignal }) {
          await new Promise<void>((resolve, reject) => {
            const timer = setTimeout(resolve, 500)
            options?.signal?.addEventListener(
              'abort',
              () => {
                clearTimeout(timer)
                reject(new DOMException('The operation was aborted', 'AbortError'))
              },
              { once: true }
            )
          })
        },
        async close() {},
      }
      const crossflight = createCrossflight({
        cache: new MemoryCache(),
        coordinator,
        defaultTimeoutMs: 40,
        onEvent: event => events.push(event as ObservedEvent),
      })

      await expect(
        crossflight.wrap('dedupe:timeout:waiter', async () => 'value')
      ).rejects.toBeInstanceOf(CoordinationTimeoutError)

      // The caller settles at its deadline; the aborted flight reports its
      // failure asynchronously once it observes the abort.
      await vi.waitFor(() => {
        expect(failedEvents(events)).toHaveLength(1)
      })
      expect(failedEvents(events)[0]!.error).toBeInstanceOf(
        CoordinationTimeoutError
      )

      await crossflight.close()
    })

    it('emits exactly one failed event when close() aborts an in-flight owner', async () => {
      const events: ObservedEvent[] = []
      const coordinator = {
        async acquire(key: string) {
          return {
            key,
            async renew() { return true },
            async complete() {},
            async abandon() {},
          }
        },
        async waitForChange() {},
        async close() {},
      }
      const crossflight = createCrossflight({
        cache: new MemoryCache(),
        coordinator,
        onEvent: event => events.push(event as ObservedEvent),
      })

      const pending = crossflight.wrap('dedupe:close:abort', async () => {
        await new Promise(resolve => setTimeout(resolve, 60))
        return 'value'
      })

      await crossflight.close()

      await expect(pending).rejects.toBeInstanceOf(CoordinationClosedError)

      const failed = failedEvents(events)
      expect(failed).toHaveLength(1)
      expect(failed[0]!.error).toBeInstanceOf(CoordinationClosedError)
    })

    it('emits exactly one failed event with the caller reason when the signal is pre-aborted', async () => {
      const events: ObservedEvent[] = []
      const controller = new AbortController()
      controller.abort(new Error('pre-aborted'))
      const crossflight = createCrossflight({
        cache: new MemoryCache(),
        coordinator: new InMemoryCoordinator(),
        onEvent: event => events.push(event as ObservedEvent),
      })

      await expect(
        crossflight.wrap('dedupe:pre:aborted', async () => 'value', {
          signal: controller.signal,
        })
      ).rejects.toThrow('pre-aborted')

      // The caller settles at once; the aborted flight reports when it settles.
      await vi.waitFor(() => {
        expect(failedEvents(events)).toHaveLength(1)
      })
      expect((failedEvents(events)[0]!.error as Error).message).toBe(
        'pre-aborted'
      )

      await crossflight.close()
    })

    it('emits exactly one failed event on confirmed ownership loss', async () => {
      const cache = new MemoryCache()
      const events: ObservedEvent[] = []
      let acquireCalls = 0
      let waitCalls = 0
      const coordinator = {
        async acquire(key: string) {
          acquireCalls += 1
          if (acquireCalls === 1) {
            return {
              key,
              async renew() { return false },
              async complete() {},
              async abandon() {},
            }
          }
          return null
        },
        async waitForChange() {
          waitCalls += 1
          if (waitCalls === 1) {
            await cache.set('dedupe:ownership:lost', 'recovered-value')
          }
        },
        async close() {},
      }
      const crossflight = createCrossflight({
        cache,
        coordinator,
        onEvent: event => events.push(event as ObservedEvent),
      })

      const result = await crossflight.wrap(
        'dedupe:ownership:lost',
        async () => 'original'
      )

      expect(result).toBe('recovered-value')

      const failed = failedEvents(events)
      expect(failed).toHaveLength(1)
      expect(failed[0]!.error).toBeInstanceOf(OwnershipLostError)

      await crossflight.close()
    })

    it('keeps exactly one failed event on a recovered fail-open fallback', async () => {
      const events: ObservedEvent[] = []
      const coordinator = {
        async acquire(): Promise<never> {
          throw new Error('acquire boom')
        },
        async waitForChange() {},
        async close() {},
      }
      const crossflight = createCrossflight({
        cache: new MemoryCache(),
        coordinator,
        failureMode: 'fail-open',
        onEvent: event => events.push(event as ObservedEvent),
      })

      await expect(
        crossflight.wrap('dedupe:fail:open:recovered', async () => 'fallback-value')
      ).resolves.toBe('fallback-value')

      const failed = failedEvents(events)
      expect(failed).toHaveLength(1)
      expect((failed[0]!.error as Error).message).toBe('acquire boom')

      await crossflight.close()
    })
  })

  const runLifecycleChild = (script: string) =>
    spawnSync(process.execPath, ['--import=tsx', '--eval', script], {
      cwd: process.cwd(),
      timeout: 10_000,
      encoding: 'utf8',
    })

  it('does not keep an exiting process alive for a pending renewal timer', () => {
    const script = `
      import { createCrossflight } from './src/index.ts'

      const coordinator = {
        async acquire(key) {
          return {
            key,
            async renew() { return true },
            async complete() {},
            async abandon() {},
          }
        },
        async waitForChange() {},
        async close() {},
      }

      const cache = {
        async get() { return { hit: false } },
        async set() {},
      }

      const crossflight = createCrossflight({ cache, coordinator })
      void crossflight.wrap('lifecycle:renewal:key', () => new Promise(() => {}), { ttl: 3000 })
    `

    const result = runLifecycleChild(script)

    expect(result.signal).toBeNull()
    expect(result.status).toBe(0)
  })

  it('does not keep an exiting process alive for a pending per-call timeout', () => {
    const script = `
      import { createCrossflight } from './src/index.ts'

      const coordinator = {
        async acquire() { throw new Error('coordinator down') },
        async waitForChange() {},
        async close() {},
      }

      const cache = {
        async get() { return { hit: false } },
        async set() {},
      }

      const crossflight = createCrossflight({ cache, coordinator })
      void crossflight.wrap('lifecycle:timeout:key', () => new Promise(() => {}), { failureMode: 'fail-open', timeoutMs: 60000 })
    `

    const result = runLifecycleChild(script)

    expect(result.signal).toBeNull()
    expect(result.status).toBe(0)
  })

  describe('caller-scoped cancellation', () => {
    type ObservedEvent = { type: string; key?: string; error?: unknown }

    const failedEvents = (events: ObservedEvent[]) =>
      events.filter(event => event.type === 'failed')

    const deferred = () => {
      let resolve!: () => void
      const promise = new Promise<void>(res => {
        resolve = res
      })
      return { promise, resolve }
    }

    const abortableWaiter = () => ({
      async acquire() {
        return null // another owner always holds the key
      },
      async waitForChange(_key: string, options?: { signal?: AbortSignal }) {
        await new Promise<void>((resolve, reject) => {
          const timer = setTimeout(resolve, 500)
          options?.signal?.addEventListener(
            'abort',
            () => {
              clearTimeout(timer)
              reject(new DOMException('The operation was aborted', 'AbortError'))
            },
            { once: true }
          )
        })
      },
      async close() {},
    })

    it('rejects only the cancelling joiner while the owner keeps waiting', async () => {
      const cache = new MemoryCache()
      const coordinator = new InMemoryCoordinator()
      const crossflight = createCrossflight({ cache, coordinator })
      const gate = deferred()
      let loadRuns = 0

      const owner = crossflight.wrap('cancel:joiner', async () => {
        loadRuns += 1
        await gate.promise
        return 'shared-value'
      })

      const controller = new AbortController()
      const joiner = crossflight.wrap(
        'cancel:joiner',
        async () => 'joiner-value',
        { signal: controller.signal }
      )

      controller.abort(new Error('joiner-cancelled'))
      gate.resolve()

      await expect(joiner).rejects.toThrow('joiner-cancelled')
      await expect(owner).resolves.toBe('shared-value')
      expect(loadRuns).toBe(1)
      await expect(cache.get('cancel:joiner')).resolves.toEqual({
        hit: true,
        value: 'shared-value',
      })

      await crossflight.close()
    })

    it('keeps a joined caller alive when the owner cancels', async () => {
      const cache = new MemoryCache()
      const coordinator = new InMemoryCoordinator()
      const crossflight = createCrossflight({ cache, coordinator })
      const gate = deferred()

      const controller = new AbortController()
      const owner = crossflight.wrap(
        'cancel:owner',
        async () => {
          await gate.promise
          return 'shared-value'
        },
        { signal: controller.signal }
      )
      const joiner = crossflight.wrap('cancel:owner', async () => 'joiner-value')

      controller.abort(new Error('owner-cancelled'))
      gate.resolve()

      await expect(owner).rejects.toThrow('owner-cancelled')
      await expect(joiner).resolves.toBe('shared-value')

      await crossflight.close()
    })

    it('applies a per-call timeout to a joined caller only', async () => {
      const cache = new MemoryCache()
      const coordinator = new InMemoryCoordinator()
      const crossflight = createCrossflight({ cache, coordinator })

      const owner = crossflight.wrap('cancel:timeout', async () => {
        await new Promise(resolve => setTimeout(resolve, 120))
        return 'shared-value'
      })
      const joiner = crossflight.wrap(
        'cancel:timeout',
        async () => 'joiner-value',
        { timeoutMs: 20 }
      )

      const error = await joiner.catch(e => e)

      expect(error).toBeInstanceOf(CoordinationTimeoutError)
      expect((error as CoordinationTimeoutError).key).toBe('cancel:timeout')
      await expect(owner).resolves.toBe('shared-value')

      await crossflight.close()
    })

    it('applies defaultTimeoutMs to a joined caller', async () => {
      const cache = new MemoryCache()
      const coordinator = new InMemoryCoordinator()
      const crossflight = createCrossflight({
        cache,
        coordinator,
        defaultTimeoutMs: 20,
      })

      const owner = crossflight.wrap(
        'cancel:default',
        async () => {
          await new Promise(resolve => setTimeout(resolve, 120))
          return 'shared-value'
        },
        { timeoutMs: 400 }
      )
      const joiner = crossflight.wrap('cancel:default', async () => 'joiner-value')

      await expect(joiner).rejects.toBeInstanceOf(CoordinationTimeoutError)
      await expect(owner).resolves.toBe('shared-value')

      await crossflight.close()
    })

    it('propagates a shared failure to a caller that set its own timeout', async () => {
      const cache = new MemoryCache()
      const coordinator = new InMemoryCoordinator()
      const crossflight = createCrossflight({ cache, coordinator })

      const error = await crossflight
        .wrap(
          'cancel:propagate',
          async () => {
            throw new Error('loader boom')
          },
          { timeoutMs: 1000 }
        )
        .catch(e => e)

      expect(error).toBeInstanceOf(Error)
      expect((error as Error).message).toBe('loader boom')

      await crossflight.close()
    })

    it('cancels the shared flight once the last caller leaves', async () => {
      const events: ObservedEvent[] = []
      const crossflight = createCrossflight({
        cache: new MemoryCache(),
        coordinator: abortableWaiter(),
        onEvent: event => events.push(event as ObservedEvent),
      })

      const ownerController = new AbortController()
      const joinerController = new AbortController()
      const owner = crossflight.wrap('cancel:last', async () => 'never', {
        signal: ownerController.signal,
      })
      const joiner = crossflight.wrap('cancel:last', async () => 'never', {
        signal: joinerController.signal,
      })

      ownerController.abort(new Error('owner-cancelled'))
      await expect(owner).rejects.toThrow('owner-cancelled')

      // One caller left, so the shared flight is still running.
      expect(failedEvents(events)).toHaveLength(0)

      joinerController.abort(new Error('joiner-cancelled'))
      await expect(joiner).rejects.toThrow('joiner-cancelled')

      // The joiner settles at once; the aborted flight reports when it settles.
      await vi.waitFor(() => {
        expect(failedEvents(events)).toHaveLength(1)
      })
      expect((failedEvents(events)[0]!.error as Error).message).toBe(
        'joiner-cancelled'
      )

      await crossflight.close()
    })

    it('settles a sole caller at its deadline even when the loader ignores the abort', async () => {
      const cache = new MemoryCache()
      const coordinator = new InMemoryCoordinator()
      const crossflight = createCrossflight({ cache, coordinator })

      const error = await crossflight
        .wrap('cancel:hang', () => new Promise(() => {}), { timeoutMs: 20 })
        .catch(e => e)

      expect(error).toBeInstanceOf(CoordinationTimeoutError)
      expect((error as CoordinationTimeoutError).key).toBe('cancel:hang')

      await crossflight.close()
    })

    it('starts a fresh flight instead of joining an aborted one', async () => {
      const cache = new MemoryCache()
      const coordinator = new InMemoryCoordinator()
      const crossflight = createCrossflight({ cache, coordinator })
      const started = deferred()
      const gate = deferred()
      let loadRuns = 0

      const controller = new AbortController()
      const first = crossflight.wrap(
        'cancel:aborted-join',
        async () => {
          loadRuns += 1
          started.resolve()
          await gate.promise
          return 'first-value'
        },
        { signal: controller.signal }
      )

      // Cancel while the loader is still running, so the aborted flight stays
      // in flight and ignores the abort signal.
      await started.promise
      controller.abort(new Error('first-cancelled'))
      await expect(first).rejects.toThrow('first-cancelled')

      // This must not join the aborted flight and inherit its cancellation.
      const second = crossflight.wrap('cancel:aborted-join', async () => {
        loadRuns += 1
        return 'second-value'
      })

      gate.resolve()

      await expect(second).resolves.toBe('second-value')
      expect(loadRuns).toBe(2)

      await crossflight.close()
    })

    it('does not apply the owner timeout to a shared ownership-loss retry', async () => {
      const cache = new MemoryCache()
      let acquireCalls = 0
      const coordinator = {
        async acquire(key: string) {
          acquireCalls += 1
          if (acquireCalls === 1) {
            return {
              key,
              async renew() {
                return false // ownership lost right after the load
              },
              async complete() {},
              async abandon() {},
            }
          }
          return null // the retry waits for another owner
        },
        async waitForChange() {
          await new Promise(resolve => setTimeout(resolve, 120))
          await cache.set('cancel:retry', 'recovered-value')
        },
        async close() {},
      }
      const crossflight = createCrossflight({ cache, coordinator })

      const owner = crossflight.wrap('cancel:retry', async () => 'original', {
        timeoutMs: 30,
      })
      const joiner = crossflight.wrap('cancel:retry', async () => 'original', {
        timeoutMs: 5000,
      })

      await expect(owner).rejects.toBeInstanceOf(CoordinationTimeoutError)
      await expect(joiner).resolves.toBe('recovered-value')

      await crossflight.close()
    })

    it('rejects every waiting caller with one failure when close aborts a shared flight', async () => {
      const events: ObservedEvent[] = []
      const coordinator = {
        async acquire(key: string) {
          return {
            key,
            async renew() {
              return true
            },
            async complete() {},
            async abandon() {},
          }
        },
        async waitForChange() {},
        async close() {},
      }
      const crossflight = createCrossflight({
        cache: new MemoryCache(),
        coordinator,
        onEvent: event => events.push(event as ObservedEvent),
      })

      const owner = crossflight.wrap('cancel:close', async () => {
        await new Promise(resolve => setTimeout(resolve, 60))
        return 'value'
      })
      const joiner = crossflight.wrap('cancel:close', async () => 'value')

      await crossflight.close()

      await expect(owner).rejects.toBeInstanceOf(CoordinationClosedError)
      await expect(joiner).rejects.toBeInstanceOf(CoordinationClosedError)

      const failed = failedEvents(events)
      expect(failed).toHaveLength(1)
      expect(failed[0]!.error).toBeInstanceOf(CoordinationClosedError)
    })

    it('releases the caller abort listener and timeout timer when the call completes', async () => {
      vi.useFakeTimers()

      try {
        const cache = new MemoryCache()
        const coordinator = new InMemoryCoordinator()
        const crossflight = createCrossflight({ cache, coordinator })
        const controller = new AbortController()

        await crossflight.wrap('resources:key', async () => 'value', {
          signal: controller.signal,
          timeoutMs: 1000,
        })

        expect(getEventListeners(controller.signal, 'abort')).toHaveLength(0)
        expect(vi.getTimerCount()).toBe(0)

        await crossflight.close()
      } finally {
        vi.useRealTimers()
      }
    })
  })

  describe('abort-aware loading', () => {
    const deferred = () => {
      let resolve!: () => void
      const promise = new Promise<void>(res => {
        resolve = res
      })
      return { promise, resolve }
    }

    it('passes the flight signal to the loader and aborts it on close', async () => {
      const cache = new MemoryCache()
      const coordinator = new InMemoryCoordinator()
      const crossflight = createCrossflight({ cache, coordinator })
      const started = deferred()
      let loaderSignal: AbortSignal | undefined

      const pending = crossflight.wrap('signal:load', async (signal) => {
        loaderSignal = signal
        started.resolve()
        await new Promise(() => {})
        return 'value'
      })

      await started.promise
      expect(loaderSignal?.aborted).toBe(false)

      await crossflight.close()

      expect(loaderSignal?.aborted).toBe(true)
      await expect(pending).rejects.toBeInstanceOf(CoordinationClosedError)
    })

    it('stops waiting for a loader that ignores the abort when the lease is lost', async () => {
      const cache = new MemoryCache()
      let abandonCalls = 0
      const coordinator = {
        async acquire(key: string) {
          return {
            key,
            async renew() {
              return false
            },
            async complete() {},
            async abandon() {
              abandonCalls += 1
            },
          }
        },
        async waitForChange() {},
        async close() {},
      }
      const crossflight = createCrossflight({ cache, coordinator })
      const t0 = Date.now()

      const error = await crossflight
        .wrap(
          'lost:lease',
          async () => {
            await new Promise(() => {})
            return 'value'
          },
          { ttl: 30, timeoutMs: 2000 }
        )
        .catch(e => e)

      expect(error).toBeInstanceOf(OwnershipLostError)
      expect(Date.now() - t0).toBeLessThan(500)
      expect(abandonCalls).toBeGreaterThanOrEqual(1)

      await crossflight.close()
    })

    it('does not run the fail-open fallback once the flight is already aborted', async () => {
      const cache = new MemoryCache()
      let loadRuns = 0
      const coordinator = {
        async acquire() {
          return null
        },
        async waitForChange() {},
        async close() {},
      }
      const crossflight = createCrossflight({
        cache,
        coordinator,
        failureMode: 'fail-open',
      })
      const controller = new AbortController()
      controller.abort(new Error('gone'))

      const error = await crossflight
        .wrap(
          'fail:open:aborted',
          async () => {
            loadRuns += 1
            return 'value'
          },
          { signal: controller.signal }
        )
        .catch(e => e)

      expect((error as Error).message).toBe('gone')
      expect(loadRuns).toBe(0)

      await crossflight.close()
    })

    it('holds ownership until an in-flight cache write lands', async () => {
      const store = new Map<string, unknown>()
      const writes: string[] = []
      let releaseFirst: (() => void) | undefined
      const cache = {
        async get() {
          return { hit: false as const }
        },
        async set<T>(key: string, value: T) {
          writes.push('start:' + String(value))
          if (!releaseFirst) {
            await new Promise<void>(resolve => {
              releaseFirst = resolve
            })
          }
          store.set(key, value)
          writes.push('land:' + String(value))
        },
      }
      const coordinator = new InMemoryCoordinator()
      const crossflight = createCrossflight({ cache, coordinator })
      const controller = new AbortController()

      const first = crossflight
        .wrap('write:race', async () => 'old', { signal: controller.signal })
        .catch(e => e)

      while (writes.length === 0) {
        await new Promise(resolve => setTimeout(resolve, 5))
      }

      controller.abort(new Error('cancelled'))
      const second = crossflight.wrap('write:race', async () => 'new')

      await new Promise(resolve => setTimeout(resolve, 60))
      releaseFirst?.()

      const [firstResult, secondResult] = await Promise.all([first, second])
      expect((firstResult as Error).message).toBe('cancelled')
      expect(secondResult).toBe('new')
      expect([...store.values()]).toEqual(['new'])
      expect(writes).toEqual(['start:old', 'land:old', 'start:new', 'land:new'])
    })

  })
})

