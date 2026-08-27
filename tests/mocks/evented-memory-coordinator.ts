import type {
  AcquireOptions,
  Coordinator,
  Lease,
  WaitOptions,
} from '../../src/types.js'

class EventedMemoryLease implements Lease {
  constructor(
    public readonly key: string,
    private readonly ownerToken: string,
    private readonly coordinator: EventedMemoryCoordinator,
    private readonly ttlMs: number
  ) {}

  async renew(): Promise<boolean> {
    const current = this.coordinator.owners.get(this.key)
    if (!current || current.ownerToken !== this.ownerToken) {
      return false
    }

    if (current.expiresAt <= Date.now()) {
      this.coordinator.clearLease(this.key, this.ownerToken)
      return false
    }

    current.expiresAt = Date.now() + this.ttlMs
    this.coordinator.notify(this.key)
    return true
  }

  async complete(): Promise<void> {
    this.coordinator.clearLease(this.key, this.ownerToken)
  }

  async abandon(): Promise<void> {
    this.coordinator.clearLease(this.key, this.ownerToken)
  }
}

export class EventedMemoryCoordinator implements Coordinator {
  readonly owners = new Map<string, { ownerToken: string; expiresAt: number }>()
  private readonly watchers = new Map<string, Set<() => void>>()
  private closed = false

  async acquire(key: string, options?: AcquireOptions): Promise<Lease | null> {
    if (this.closed) {
      throw new Error('Coordinator is closed')
    }

    if (options?.signal?.aborted) {
      throw new DOMException('The operation was aborted', 'AbortError')
    }

    const ttlMs = options?.ttlMs ?? 30_000
    const now = Date.now()
    const current = this.owners.get(key)

    if (current && current.expiresAt > now) {
      return null
    }

    const ownerToken = `${key}:${Math.random().toString(36).slice(2)}`
    this.owners.set(key, {
      ownerToken,
      expiresAt: now + ttlMs,
    })

    this.notify(key)
    return new EventedMemoryLease(key, ownerToken, this, ttlMs)
  }

  async waitForChange(key: string, options?: WaitOptions): Promise<void> {
    if (this.closed) {
      throw new Error('Coordinator is closed')
    }

    if (options?.signal?.aborted) {
      throw new DOMException('The operation was aborted', 'AbortError')
    }

    const timeoutMs = options?.timeoutMs ?? 50
    const signal = options?.signal

    return await new Promise<void>((resolve, reject) => {
      const set = this.watchers.get(key) ?? new Set<() => void>()
      const notify = () => {
        cleanup()
        resolve()
      }

      const onAbort = () => {
        cleanup()
        reject(new DOMException('The operation was aborted', 'AbortError'))
      }

      const cleanup = () => {
        clearTimeout(timer)
        signal?.removeEventListener('abort', onAbort)
        set.delete(notify)
        if (set.size === 0) {
          this.watchers.delete(key)
        }
      }

      const timer = setTimeout(() => {
        cleanup()
        resolve()
      }, timeoutMs)

      set.add(notify)
      this.watchers.set(key, set)
      signal?.addEventListener('abort', onAbort, { once: true })
    })
  }

  async close(): Promise<void> {
    this.closed = true
    this.owners.clear()
    this.watchers.clear()
  }

  notify(key: string): void {
    if (this.closed) {
      return
    }

    const listeners = this.watchers.get(key)
    if (!listeners) {
      return
    }

    for (const listener of [...listeners]) {
      listener()
    }
  }

  clearLease(key: string, ownerToken: string): void {
    const current = this.owners.get(key)
    if (current && current.ownerToken === ownerToken) {
      this.owners.delete(key)
      this.notify(key)
    }
  }
}

export function eventedMemoryCoordinator(): Coordinator {
  return new EventedMemoryCoordinator()
}
