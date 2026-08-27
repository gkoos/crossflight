import type {
  AcquireOptions,
  Coordinator,
  Lease,
  WaitOptions,
} from '../../src/types.js'

class InMemoryLease implements Lease {
  constructor(
    public readonly key: string,
    private readonly ownerToken: string,
    private readonly coordinator: InMemoryCoordinator,
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

    return new InMemoryLease(key, ownerToken, this, ttlMs)
  }

  async waitForChange(_key: string, options?: WaitOptions): Promise<void> {
    const timeoutMs = options?.timeoutMs ?? 50
    const signal = options?.signal

    if (signal?.aborted) {
      throw new DOMException('The operation was aborted', 'AbortError')
    }

    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        cleanup()
        resolve()
      }, timeoutMs)

      const onAbort = () => {
        cleanup()
        reject(new DOMException('The operation was aborted', 'AbortError'))
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

  isCurrentOwner(key: string, ownerToken: string): boolean {
    const current = this.owners.get(key)
    return Boolean(
      current &&
      current.ownerToken === ownerToken &&
      current.expiresAt > Date.now()
    )
  }

  clearLease(key: string, ownerToken: string): void {
    const current = this.owners.get(key)

    if (current && current.ownerToken === ownerToken) {
      this.owners.delete(key)
    }
  }
}
