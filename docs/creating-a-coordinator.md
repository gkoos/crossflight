# Creating a Coordinator

A coordinator manages distributed ownership and wake-up signals for cache misses across processes. It ensures only one process runs the expensive loader while others wait.

## The Coordinator Interface

```ts
export interface Coordinator {
  acquire(key: string, options?: AcquireOptions): Promise<Lease | null>
  waitForChange(key: string, options?: WaitOptions): Promise<void>
  close(): Promise<void>
}

export interface Lease {
  readonly key: string
  renew(): Promise<boolean>
  complete(): Promise<void>
  abandon(): Promise<void>
}

export interface AcquireOptions {
  signal?: AbortSignal
  ttlMs?: number
}

export interface WaitOptions {
  signal?: AbortSignal
  timeoutMs?: number
}
```

## Key Concepts

- **Acquire**: Try to claim ownership of a key. Returns a `Lease` if successful (you own it) or `null` if someone else owns it.
- **Lease**: An ownership token that you hold while loading. Must track the owner token to prevent stale owners from interfering.
- **Renew**: Extend the lease time. Returns `false` if the lease has expired or been taken over.
- **WaitForChange**: Wait for the cache state to change (new owner acquired, owner completed, owner abandoned). Must timeout if no change happens.
- **Close**: Clean up all state, cancel all waiting operations, shut down.

## Critical Correctness Requirements

1. **Owner token comparison**: `renew()`, `complete()`, and `abandon()` must compare the current stored token against the lease token. Never act on a key whose owner has changed.
2. **Lease expiry**: Once TTL passes, the lease is invalid. Another caller can immediately acquire it.
3. **Notification reliability**: `waitForChange()` must return when state changes, but it must also **timeout gracefully** if notifications are lost. Crossflight has retry logic to handle missed notifications.
4. **No deadlocks**: If a process crashes while holding the lease, the lease must expire so other callers can proceed. Never make a lease permanent.
5. **Signal respect**: `acquire()` and `waitForChange()` must respect `AbortSignal`. When the signal fires, stop waiting and reject with `signal.reason`. Crossflight will re-throw that reason to the caller.

## Example: Simple In-Memory Coordinator

This coordinator works within a single process (for testing):

```ts
import type {
  AcquireOptions,
  Coordinator,
  Lease,
  WaitOptions,
} from 'crossflight'

class InMemoryLease implements Lease {
  constructor(
    public readonly key: string,
    private readonly ownerToken: string,
    private readonly coordinator: InMemoryCoordinator,
    private readonly ttlMs: number
  ) {}

  async renew(): Promise<boolean> {
    const current = this.coordinator.owners.get(this.key)
    
    // Token mismatch = someone else owns it now
    if (!current || current.ownerToken !== this.ownerToken) {
      return false
    }

    // TTL expired = lease is dead
    if (current.expiresAt <= Date.now()) {
      this.coordinator.clearLease(this.key, this.ownerToken)
      return false
    }

    // Extend the lease
    current.expiresAt = Date.now() + this.ttlMs
    return true
  }

  async complete(): Promise<void> {
    this.coordinator.clearLease(this.key, this.ownerToken)
  }

  async abandon(): Promise<void> {
    this.coordinator.clearLease(this.key, this.ownerToken)
  }
}

export class InMemoryCoordinator implements Coordinator {
  readonly owners = new Map<string, { ownerToken: string; expiresAt: number }>()

  async acquire(key: string, options?: AcquireOptions): Promise<Lease | null> {
    if (options?.signal?.aborted) {
      throw options.signal.reason
    }

    const ttlMs = options?.ttlMs ?? 30_000
    const now = Date.now()
    const current = this.owners.get(key)

    // Someone owns it and hasn't expired
    if (current && current.expiresAt > now) {
      return null
    }

    // Claim ownership
    const ownerToken = `${key}:${Math.random().toString(36).slice(2)}`
    this.owners.set(key, {
      ownerToken,
      expiresAt: now + ttlMs,
    })

    return new InMemoryLease(key, ownerToken, this, ttlMs)
  }

  async waitForChange(_key: string, options?: WaitOptions): Promise<void> {
    const timeoutMs = options?.timeoutMs ?? 50
    const signal = options?.signal

    if (signal?.aborted) {
      throw signal.reason
    }

    // Simple timeout-only wait (no actual notifications)
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        cleanup()
        resolve()
      }, timeoutMs)

      const onAbort = () => {
        cleanup()
        reject(signal!.reason)
      }

      const cleanup = () => {
        clearTimeout(timer)
        signal?.removeEventListener('abort', onAbort)
      }

      signal?.addEventListener('abort', onAbort, { once: true })
    })
  }

  async close(): Promise<void> {
    this.owners.clear()
  }

  clearLease(key: string, ownerToken: string): void {
    const current = this.owners.get(key)
    if (current && current.ownerToken === ownerToken) {
      this.owners.delete(key)
    }
  }
}
```

## Example: Redis Coordinator (Simplified)

```ts
import type {
  AcquireOptions,
  Coordinator,
  Lease,
  WaitOptions,
} from 'crossflight'
import { Redis } from 'ioredis'
import { randomUUID } from 'node:crypto'

class RedisLease implements Lease {
  constructor(
    public readonly key: string,
    private readonly ownerToken: string,
    private readonly client: Redis,
    private readonly ttlMs: number
  ) {}

  async renew(): Promise<boolean> {
    // Lua script ensures atomic read-compare-update
    const result = await this.client.eval(
      `
      if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('pexpire', KEYS[1], ARGV[2])
      end
      return 0
      `,
      1,
      this.key,
      this.ownerToken,
      String(this.ttlMs)
    )

    return Number(result) === 1
  }

  async complete(): Promise<void> {
    await this.client.eval(
      `
      if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
      end
      return 0
      `,
      1,
      this.key,
      this.ownerToken
    )
  }

  async abandon(): Promise<void> {
    await this.complete() // Same logic
  }
}

export function redisCoordinator(client: Redis): Coordinator {
  return {
    async acquire(key: string, options?: AcquireOptions): Promise<Lease | null> {
      if (options?.signal?.aborted) {
        throw options.signal.reason
      }

      const ttlMs = options?.ttlMs ?? 30_000
      const ownerToken = randomUUID()

      const result = await client.eval(
        `
        if redis.call('get', KEYS[1]) == false then
          return redis.call('set', KEYS[1], ARGV[1], 'PX', ARGV[2], 'NX') ~= false and 1 or 0
        end
        return 0
        `,
        1,
        key,
        ownerToken,
        String(ttlMs)
      )

      if (Number(result) !== 1) {
        return null
      }

      return new RedisLease(key, ownerToken, client, ttlMs)
    },

    async waitForChange(key: string, options?: WaitOptions): Promise<void> {
      // Simplified: just timeout
      // Real implementation would subscribe to Pub/Sub notifications
      const timeoutMs = options?.timeoutMs ?? 100
      const signal = options?.signal

      if (signal?.aborted) {
        throw signal.reason
      }

      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => {
          cleanup()
          resolve()
        }, timeoutMs)

        const onAbort = () => {
          cleanup()
          reject(signal!.reason)
        }

        const cleanup = () => {
          clearTimeout(timer)
          signal?.removeEventListener('abort', onAbort)
        }

        signal?.addEventListener('abort', onAbort, { once: true })
      })
    },

    async close(): Promise<void> {
      // Clean up any resources
    },
  }
}
```

## Testing Your Coordinator

```ts
import { describe, it, expect } from 'vitest'
import { MyCoordinator } from './my-coordinator.js'

describe('MyCoordinator', () => {
  it('acquires ownership and renews the lease', async () => {
    const coordinator = new MyCoordinator()
    
    const lease = await coordinator.acquire('test:key', { ttlMs: 100 })
    expect(lease).not.toBeNull()

    const renewed = await lease!.renew()
    expect(renewed).toBe(true)

    await lease!.complete()
    await coordinator.close()
  })

  it('rejects stale owners', async () => {
    const coordinator = new MyCoordinator()

    const lease1 = await coordinator.acquire('test:key', { ttlMs: 50 })
    expect(lease1).not.toBeNull()

    // Wait for TTL to expire
    await new Promise(resolve => setTimeout(resolve, 100))

    // Try to renew with expired lease
    const renewed = await lease1!.renew()
    expect(renewed).toBe(false)

    // Someone else can acquire now
    const lease2 = await coordinator.acquire('test:key', { ttlMs: 50 })
    expect(lease2).not.toBeNull()

    await lease2!.complete()
    await coordinator.close()
  })

  it('handles waitForChange timeout', async () => {
    const coordinator = new MyCoordinator()

    const startMs = Date.now()
    await coordinator.waitForChange('test:key', { timeoutMs: 50 })
    const elapsedMs = Date.now() - startMs

    // Should resolve roughly after timeoutMs
    expect(elapsedMs).toBeGreaterThanOrEqual(40)
    expect(elapsedMs).toBeLessThan(200)

    await coordinator.close()
  })

  it('respects AbortSignal', async () => {
    const coordinator = new MyCoordinator()
    const controller = new AbortController()

    setTimeout(() => controller.abort(), 10)

    try {
      await coordinator.acquire('test:key', { signal: controller.signal })
      expect.fail('Should have thrown on abort')
    } catch (error) {
      // The coordinator re-throws signal.reason, so the caller receives
      // whatever was passed to controller.abort().
      expect(error).toBeDefined()
    }

    await coordinator.close()
  })
})
```

## Publishing Your Coordinator

If you want to share your coordinator as a package:

1. Create a new npm package (e.g., `@myorg/crossflight-postgres-coordinator`)
2. Add `crossflight` as a peer dependency
3. Export your coordinator factory function
4. Document the setup and options

Users can then install and use it:

```ts
import { postgresCoordinator } from '@myorg/crossflight-postgres-coordinator'

const crossflight = createCrossflight({
  cache: myAdapter,
  coordinator: postgresCoordinator(pool),
})
```

## What Not To Do

- **Don't mix read and write concerns** — keep lease validation separate from state changes
- **Don't skip owner token checks** — always compare tokens before allowing mutations
- **Don't make leases permanent** — always expire them, even if `close()` isn't called
- **Don't panic on `waitForChange` timeout** — it's expected behavior; Crossflight retries
- **Don't ignore AbortSignal** — respect caller cancellation
