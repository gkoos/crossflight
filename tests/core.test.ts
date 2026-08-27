import { describe, expect, it } from 'vitest'

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

  it('returns cached value found during ownership recheck without running the loader', async () => {
    const cache = new MemoryCache()
    let leaseAcquired = false
    let loaderRan = false

    const coordinator = {
      async acquire(key: string) {
        if (leaseAcquired) return null
        leaseAcquired = true
        // Populate cache between acquire and loader so recheck hits
        await cache.set(key, 'populated-between')
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

    const crossflight = createCrossflight({ cache, coordinator })

    const result = await crossflight.wrap('recheck:hit:key', async () => {
      loaderRan = true
      return 'from-loader'
    })

    expect(result).toBe('populated-between')
    expect(loaderRan).toBe(false)
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
})
