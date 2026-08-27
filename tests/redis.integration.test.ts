import { spawnSync } from 'node:child_process'
import { createHash } from 'node:crypto'

import { Redis as IORedis } from 'ioredis'
import { describe, expect, it, vi } from 'vitest'

import { createCrossflight } from '../src/index.js'
import { redisCoordinator } from '../src/coordinators/redis.js'

const shouldRun = process.env.RUN_REDIS_INTEGRATION === '1'

function runNodeProcess(key: string) {
  const script = `
    import { Redis as IORedis } from 'ioredis';
    import { createCrossflight } from './src/core.ts';
    import { redisCoordinator } from './src/coordinators/redis.ts';

    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379');
    const cache = {
      async get(key) {
        const value = await client.get(key);
        if (value === null) {
          return { hit: false };
        }

        return { hit: true, value: JSON.parse(value) };
      },
      async set(key, value, options) {
        const payload = JSON.stringify(value);
        if (options?.ttl) {
          await client.set(key, payload, 'PX', options.ttl);
          return;
        }

        await client.set(key, payload);
      },
    };

    const crossflight = createCrossflight({
      cache,
      coordinator: redisCoordinator(client),
    });

    try {
      const result = await crossflight.wrap('${key}', async () => {
        const count = Number(await client.incr('crossflight:counter:${key}'));
        await new Promise(resolve => setTimeout(resolve, 120));
        return { value: 'computed', count };
      }, { ttl: 2000 });

      console.log(JSON.stringify({ ok: true, result }));
    } catch (error) {
      console.log(JSON.stringify({ ok: false, error: error instanceof Error ? error.message : String(error) }));
      process.exitCode = 1;
    } finally {
      await crossflight.close();
    }
  `

  return spawnSync(process.execPath, ['--import=tsx', '--eval', script], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      REDIS_URL: process.env.REDIS_URL ?? 'redis://localhost:6379',
    },
    encoding: 'utf8',
  })
}

function createRedisCache(client: IORedis) {
  return {
    async get<T>(key: string) {
      const value = await client.get(key)
      if (value === null) {
        return { hit: false as const }
      }

      return { hit: true as const, value: JSON.parse(value) as T }
    },
    async set<T>(key: string, value: T, options?: { ttl?: number }) {
      const payload = JSON.stringify(value)

      if (options?.ttl) {
        await client.set(key, payload, 'PX', options.ttl)
        return
      }

      await client.set(key, payload)
    },
  }
}

type RedisCoordinatorTestInternals = {
  subscriptionClient: IORedis
  subscribedChannels: Set<string>
}

type MutableQuitClient = IORedis & {
  quit: () => Promise<unknown>
}

type MutableEvalClient = IORedis & {
  eval: (...args: unknown[]) => Promise<unknown>
}

describe.runIf(shouldRun)('redis coordinator integration', () => {
  it('uses the redis coordinator path for a basic miss', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const cache = createRedisCache(client)

    const crossflight = createCrossflight({
      cache,
      coordinator: redisCoordinator(client),
    })

    const value = await crossflight.wrap('redis:test:key', async () => ({ hello: 'world' }), { ttl: 5000 })

    expect(value).toEqual({ hello: 'world' })
    await crossflight.close()
  })

  it('coalesces concurrent redis misses into a single loader execution', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const cache = createRedisCache(client)

    const crossflight = createCrossflight({
      cache,
      coordinator: redisCoordinator(client),
    })

    let loadRuns = 0

    const loader = async () => {
      loadRuns += 1
      await new Promise(resolve => setTimeout(resolve, 40))
      return { hello: 'world' }
    }

    const [first, second] = await Promise.all([
      crossflight.wrap('redis:coalesced:key', loader, { ttl: 5000 }),
      crossflight.wrap('redis:coalesced:key', loader, { ttl: 5000 }),
    ])

    expect(first).toEqual({ hello: 'world' })
    expect(second).toEqual({ hello: 'world' })
    expect(loadRuns).toBe(1)
    await crossflight.close()
  })

  it('allows a new lease after the previous ttl expires', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)

    const firstLease = await coordinator.acquire('redis:lease:key', { ttlMs: 50 })
    expect(firstLease).not.toBeNull()

    await new Promise(resolve => setTimeout(resolve, 120))

    const secondLease = await coordinator.acquire('redis:lease:key', { ttlMs: 100 })
    expect(secondLease).not.toBeNull()

    await secondLease?.complete()
    await coordinator.close()
  })

  it('does not allow a stale lease to revive after a newer owner already owns the key', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)

    const firstLease = await coordinator.acquire('redis:stale:revive:key', { ttlMs: 40 })
    expect(firstLease).not.toBeNull()

    await new Promise(resolve => setTimeout(resolve, 80))

    const secondLease = await coordinator.acquire('redis:stale:revive:key', { ttlMs: 200 })
    expect(secondLease).not.toBeNull()

    await firstLease!.renew()
    const staleKey = createHash('sha256').update('redis:stale:revive:key').digest('hex')
    const currentOwner = await client.get(`crossflight:flight:${staleKey}`)
    expect(currentOwner).not.toBeNull()
    expect(currentOwner).not.toBe(firstLease!.key)

    await secondLease?.complete()
    await coordinator.close()
  })

  it('notifies waiters when another process changes the same redis key', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const otherClient = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)
    const otherCoordinator = redisCoordinator(otherClient)

    const key = 'redis:waiter:notification:key'
    const channel = `crossflight:change:${createHash('sha256').update(key).digest('hex')}`

    const waiter = coordinator.waitForChange(key, { timeoutMs: 500 })
    await new Promise(resolve => setTimeout(resolve, 25))

    await otherClient.publish(channel, `${Date.now()}`)
    await expect(waiter).resolves.toBeUndefined()

    await coordinator.close()
    await otherCoordinator.close()
  })

  it('returns from waitForChange even when no notification arrives', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)

    const lease = await coordinator.acquire('redis:waiter:missed:notification', { ttlMs: 200 })
    expect(lease).not.toBeNull()

    const wait = coordinator.waitForChange('redis:waiter:missed:notification', { timeoutMs: 50 })

    // Guard against regressions where waitForChange resolves immediately
    // without waiting for either a message or timeout.
    const earlyProbe = Promise.race([
      wait.then(() => 'resolved' as const),
      new Promise<'probe-timeout'>(resolve => setTimeout(() => resolve('probe-timeout'), 10)),
    ])
    await expect(earlyProbe).resolves.toBe('probe-timeout')

    await expect(wait).resolves.toBeUndefined()

    await lease?.complete()
    await coordinator.close()
  })

  it('allows a fresh coordinator to acquire after the old lease expires, even if the old coordinator was closed', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)

    const firstLease = await coordinator.acquire('redis:close:reacquire:key', { ttlMs: 80 })
    expect(firstLease).not.toBeNull()

    await coordinator.close()
    await new Promise(resolve => setTimeout(resolve, 150))

    const freshClient = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const freshCoordinator = redisCoordinator(freshClient)
    const secondLease = await freshCoordinator.acquire('redis:close:reacquire:key', { ttlMs: 100 })
    expect(secondLease).not.toBeNull()

    await secondLease?.complete()
    await freshCoordinator.close()
  })

  it('does not let a stale lease delete a newer owner', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)

    const firstLease = await coordinator.acquire('redis:stale:key', { ttlMs: 50 })
    expect(firstLease).not.toBeNull()

    await new Promise(resolve => setTimeout(resolve, 120))

    const secondLease = await coordinator.acquire('redis:stale:key', { ttlMs: 200 })
    expect(secondLease).not.toBeNull()

    await firstLease?.complete()
    const staleKey = createHash('sha256').update('redis:stale:key').digest('hex')
    const ownerValueAfterStaleComplete = await client.get(`crossflight:flight:${staleKey}`)
    expect(ownerValueAfterStaleComplete).not.toBeNull()

    await secondLease?.complete()
    await coordinator.close()
  })

  it('returns null when a key is already leased by another owner', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)

    const key = 'redis:already-leased:key'
    const firstLease = await coordinator.acquire(key, { ttlMs: 1000 })
    expect(firstLease).not.toBeNull()

    const secondLease = await coordinator.acquire(key, { ttlMs: 1000 })
    expect(secondLease).toBeNull()

    await firstLease?.complete()
    await coordinator.close()
  })

  it('uses a namespaced, hashed Redis key for lease ownership', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client, {
      namespace: 'demo-ns',
      hashKey: key => `hashed:${key}`,
    })

    const lease = await coordinator.acquire('redis:namespace:key', { ttlMs: 500 })
    expect(lease).not.toBeNull()

    const matchingKeys = await client.keys('demo-ns:flight:*')
    expect(matchingKeys).toHaveLength(1)
    expect(matchingKeys[0]).toBe('demo-ns:flight:hashed:redis:namespace:key')

    await lease?.complete()
    await coordinator.close()
  })

  it('rejects new acquisition attempts after the coordinator is closed', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)

    await coordinator.close()

    await expect(coordinator.acquire('redis:closed:after:close', { ttlMs: 500 })).rejects.toThrow(/closed/i)
  })

  it('rejects operations when the underlying redis client has already been closed externally', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)

    await client.quit()

    await expect(coordinator.acquire('redis:closed:external', { ttlMs: 500 })).rejects.toThrow(/closed|disconnected|connection/i)
    await expect(coordinator.waitForChange('redis:closed:external', { timeoutMs: 50 })).rejects.toThrow(/closed|disconnected|connection/i)
  })

  it('treats disconnected acquire errors as closed coordinator errors', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)
    const mutableClient = client as MutableEvalClient

    mutableClient.eval = async () => {
      throw new Error('socket disconnected unexpectedly')
    }

    await expect(coordinator.acquire('redis:closed:disconnected', { ttlMs: 500 })).rejects.toThrow(/closed/i)
    await expect(coordinator.acquire('redis:closed:disconnected:again', { ttlMs: 500 })).rejects.toThrow(/closed/i)

    await coordinator.close()
  })

  it('treats connection lost acquire errors as closed coordinator errors', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)
    const mutableClient = client as MutableEvalClient

    mutableClient.eval = async () => {
      throw new Error('connection to redis was lost')
    }

    await expect(coordinator.acquire('redis:closed:lost', { ttlMs: 500 })).rejects.toThrow(/closed/i)

    await coordinator.close()
  })

  it('does not mask non-closed acquire errors that only mention connection', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)
    const mutableClient = client as MutableEvalClient

    mutableClient.eval = async () => {
      throw new Error('connection timeout while writing command')
    }

    await expect(coordinator.acquire('redis:connection:timeout', { ttlMs: 500 })).rejects.toThrow('connection timeout while writing command')

    await coordinator.close()
  })

  it('removes its redis lifecycle listeners when closed', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)

    expect(client.listenerCount('error')).toBeGreaterThan(0)
    expect(client.listenerCount('end')).toBeGreaterThan(0)

    await coordinator.close()

    expect(client.listenerCount('error')).toBe(0)
    expect(client.listenerCount('end')).toBe(0)
  })

  it('publishes a change notification when a lease is completed', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)

    const key = 'redis:complete:notify:key'
    const lease = await coordinator.acquire(key, { ttlMs: 500 })
    expect(lease).not.toBeNull()

    const waiter = coordinator.waitForChange(key, { timeoutMs: 1000 })
    await new Promise(resolve => setTimeout(resolve, 25))

    await lease!.complete()

    const startedAt = Date.now()
    await expect(waiter).resolves.toBeUndefined()
    expect(Date.now() - startedAt).toBeLessThan(500)

    await coordinator.close()
  })

  it('publishes a change notification when a lease is renewed', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)

    const key = 'redis:renew:notify:key'
    const lease = await coordinator.acquire(key, { ttlMs: 500 })
    expect(lease).not.toBeNull()

    const waiter = coordinator.waitForChange(key, { timeoutMs: 1000 })
    await new Promise(resolve => setTimeout(resolve, 25))

    const renewed = await lease!.renew()
    expect(renewed).toBe(true)

    await expect(waiter).resolves.toBeUndefined()
    await lease!.complete()
    await coordinator.close()
  })

  it('throws AbortError when acquire is called with an already-aborted signal', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)

    const controller = new AbortController()
    controller.abort()

    await expect(
      coordinator.acquire('redis:aborted:signal', { signal: controller.signal })
    ).rejects.toMatchObject({ name: 'AbortError' })

    await coordinator.close()
  })

  it('throws AbortError when waitForChange is called with an already-aborted signal', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)

    const controller = new AbortController()
    controller.abort()

    await expect(
      coordinator.waitForChange('redis:aborted:wait:signal', { signal: controller.signal, timeoutMs: 100 })
    ).rejects.toMatchObject({ name: 'AbortError' })

    await coordinator.close()
  })

  it('aborts an in-progress waitForChange when the signal fires', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)
    const controller = new AbortController()

    const waiter = coordinator.waitForChange('redis:abort:mid:wait', {
      signal: controller.signal,
      timeoutMs: 5000,
    })

    await new Promise(resolve => setTimeout(resolve, 30))
    controller.abort()

    await expect(waiter).rejects.toMatchObject({ name: 'AbortError' })
    await coordinator.close()
  })

  it('ignores pub/sub messages for a different channel', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const publishClient = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)

    const key = 'redis:channel:filter:key'
    const wrongChannel = 'crossflight:change:wrong-channel'

    // Start waiting on the real channel
    const waiter = coordinator.waitForChange(key, { timeoutMs: 200 })
    await new Promise(resolve => setTimeout(resolve, 30))

    // Publish to a different channel — should be ignored
    await publishClient.publish(wrongChannel, `${Date.now()}`)

    // Should resolve via timeout, not the wrong-channel message
    const startedAt = Date.now()
    await expect(waiter).resolves.toBeUndefined()
    expect(Date.now() - startedAt).toBeGreaterThanOrEqual(150)

    await coordinator.close()
    await publishClient.quit()
  })

  it('throws when acquire is called after the client disconnects externally', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)

    // Force the client into a closed state by quitting then trying to use it
    client.emit('end')

    await expect(
      coordinator.acquire('redis:client:ended', { ttlMs: 100 })
    ).rejects.toThrow(/closed/i)

    // Clean up without using the closed coordinator
    client.disconnect()
  })

  it('treats a client already in close status as closed', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)

    Object.defineProperty(client, 'status', {
      configurable: true,
      value: 'close',
    })

    await expect(
      coordinator.acquire('redis:status:close:key', { ttlMs: 100 })
    ).rejects.toThrow(/closed/i)

    client.disconnect()
  })

  it('publishes a change notification when a lease is abandoned', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)

    const key = 'redis:abandon:notify:key'
    const lease = await coordinator.acquire(key, { ttlMs: 500 })
    expect(lease).not.toBeNull()

    // Start waiting for change before abandoning
    const waiter = coordinator.waitForChange(key, { timeoutMs: 1000 })
    await new Promise(resolve => setTimeout(resolve, 25))

    await lease!.abandon()

    // Should resolve quickly via pub/sub notification rather than timing out
    const startedAt = Date.now()
    await expect(waiter).resolves.toBeUndefined()
    expect(Date.now() - startedAt).toBeLessThan(500)

    await coordinator.close()
  })

  it('ignores non-matching in-process message events while waiting', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)
    const internals = coordinator as unknown as RedisCoordinatorTestInternals

    const key = 'redis:channel:internal-filter:key'
    const waiter = coordinator.waitForChange(key, { timeoutMs: 200 })

    await new Promise(resolve => setTimeout(resolve, 30))
    internals.subscriptionClient.emit('message', 'crossflight:change:not-matching', 'x')

    const startedAt = Date.now()
    await expect(waiter).resolves.toBeUndefined()
    expect(Date.now() - startedAt).toBeGreaterThanOrEqual(120)

    await coordinator.close()
  })

  it('propagates subscription failures from waitForChange', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)
    const internals = coordinator as unknown as RedisCoordinatorTestInternals
    const subscriptionClient = internals.subscriptionClient
    const originalSubscribe = subscriptionClient.subscribe.bind(subscriptionClient)

    subscriptionClient.subscribe = () => Promise.reject(new Error('subscribe failed'))

    await expect(
      coordinator.waitForChange('redis:subscribe:fail:key', { timeoutMs: 100 })
    ).rejects.toThrow('subscribe failed')

    subscriptionClient.subscribe = originalSubscribe
    await coordinator.close()
  })

  it('throws when waitForChange is called on a closed coordinator', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)

    await coordinator.close()

    await expect(
      coordinator.waitForChange('redis:closed:waitForChange', { timeoutMs: 50 })
    ).rejects.toThrow(/closed/i)
  })

  it('handles double close without throwing', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)

    await coordinator.close()
    await expect(coordinator.close()).resolves.toBeUndefined()
  })

  it('unsubscribes tracked channels on close', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)
    const internals = coordinator as unknown as RedisCoordinatorTestInternals
    const subscriptionClient = internals.subscriptionClient
    const unsubscribeSpy = vi.spyOn(subscriptionClient, 'unsubscribe')

    internals.subscribedChannels.add('crossflight:change:manual')
    await coordinator.close()

    expect(unsubscribeSpy).toHaveBeenCalledWith('crossflight:change:manual')
    unsubscribeSpy.mockRestore()
  })

  it('swallows closed-client quit errors in close', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)
    const mutableClient = client as MutableQuitClient

    Object.defineProperty(client, 'status', {
      configurable: true,
      value: 'ready',
    })
    mutableClient.quit = async () => {
      throw new Error('Connection is closed')
    }

    await expect(coordinator.close()).resolves.toBeUndefined()
  })

  it('throws non-closed client quit errors in close', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)
    const mutableClient = client as MutableQuitClient

    Object.defineProperty(client, 'status', {
      configurable: true,
      value: 'ready',
    })
    mutableClient.quit = async () => {
      throw new Error('client quit failed')
    }

    await expect(coordinator.close()).rejects.toThrow('client quit failed')
    client.disconnect()
  })

  it('swallows closed subscription-client quit errors in close', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)
    const internals = coordinator as unknown as RedisCoordinatorTestInternals
    const subscriptionClient = internals.subscriptionClient

    Object.defineProperty(client, 'status', {
      configurable: true,
      value: 'end',
    })
    Object.defineProperty(subscriptionClient, 'status', {
      configurable: true,
      value: 'ready',
    })
    subscriptionClient.quit = async () => {
      throw new Error('Connection is closed')
    }

    await expect(coordinator.close()).resolves.toBeUndefined()
  })

  it('throws non-closed subscription-client quit errors in close', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const coordinator = redisCoordinator(client)
    const internals = coordinator as unknown as RedisCoordinatorTestInternals
    const subscriptionClient = internals.subscriptionClient

    Object.defineProperty(client, 'status', {
      configurable: true,
      value: 'end',
    })
    Object.defineProperty(subscriptionClient, 'status', {
      configurable: true,
      value: 'ready',
    })
    subscriptionClient.quit = async () => {
      throw new Error('subscription quit failed')
    }

    await expect(coordinator.close()).rejects.toThrow('subscription quit failed')
    client.disconnect()
    subscriptionClient.disconnect()
  })

  it('coalesces the same key across separate node processes', async () => {
    const client = new IORedis(process.env.REDIS_URL ?? 'redis://localhost:6379')
    const key = 'redis:multi-process:key'
    const flightKey = createHash('sha256').update(key).digest('hex')

    await client.del(`crossflight:flight:${flightKey}`)
    await client.del(`crossflight:counter:${key}`)

    const first = runNodeProcess(key)
    const second = runNodeProcess(key)

    expect(first.status).toBe(0)
    expect(second.status).toBe(0)

    const firstOutput = JSON.parse(first.stdout.trim())
    const secondOutput = JSON.parse(second.stdout.trim())

    expect(firstOutput.ok).toBe(true)
    expect(secondOutput.ok).toBe(true)
    expect(firstOutput.result.value).toBe('computed')
    expect(secondOutput.result.value).toBe('computed')
    expect([firstOutput.result.count, secondOutput.result.count]).toEqual([1, 1])

    await client.del(`crossflight:flight:${flightKey}`)
    await client.del(`crossflight:counter:${key}`)
    await client.quit()
  })
})
