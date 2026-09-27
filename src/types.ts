export type CacheLookup<T> = { hit: true; value: T } | { hit: false }

export interface CacheSetOptions {
  ttl?: number
}

export interface CacheAdapter {
  get<T>(key: string): Promise<CacheLookup<T>>
  set<T>(key: string, value: T, options?: CacheSetOptions): Promise<void>
}

export interface AcquireOptions {
  signal?: AbortSignal
  ttlMs?: number
}

export interface WaitOptions {
  signal?: AbortSignal
  timeoutMs?: number
}

export interface Lease {
  readonly key: string
  renew(): Promise<boolean>
  complete(): Promise<void>
  abandon(): Promise<void>
}

export interface Coordinator {
  acquire(key: string, options?: AcquireOptions): Promise<Lease | null>
  waitForChange(key: string, options?: WaitOptions): Promise<void>
  close(): Promise<void>
}

export type CoordinationFailureMode = 'fail-closed' | 'fail-open'

export type CrossflightEvent =
  | { type: 'hit'; key: string; waitedMs?: number }
  | { type: 'miss'; key: string }
  | { type: 'local_join'; key: string }
  | { type: 'distributed_join'; key: string }
  | { type: 'ownership_acquired'; key: string }
  | { type: 'cancelled'; key: string; reason: unknown }
  | { type: 'fallback'; key: string; reason: unknown }
  | { type: 'wait_exhausted'; key: string; attempts: number }
  | { type: 'completed'; key: string; durationMs: number; waitedMs?: number }
  | { type: 'failed'; key: string; error: unknown }
  | { type: 'renewal_failed'; key: string; error: unknown }

export interface WrapOptions {
  /** How long the value stays cached. Does not affect the coordination lease. */
  ttl?: number
  /** Lease TTL for this call; defaults to `defaultTtlMs`. */
  leaseTtlMs?: number
  signal?: AbortSignal
  timeoutMs?: number
  failureMode?: CoordinationFailureMode
}

/**
 * Produces the value for a cache miss. It receives the shared flight's
 * `AbortSignal`, which fires when the last waiting caller cancels, when
 * `close()` is called, or when the lease is lost. A loader doing cancellable
 * work should pass it through and stop as soon as it aborts.
 */
export type Loader<T> = (signal: AbortSignal) => Promise<T> | T

export interface Crossflight {
  wrap<T>(key: string, loader: Loader<T>, options?: WrapOptions): Promise<T>
  close(): Promise<void>
}

export interface CrossflightOptions {
  cache: CacheAdapter
  coordinator: Coordinator
  defaultTimeoutMs?: number
  defaultTtlMs?: number
  maxRetryAttempts?: number
  retryBackoff?: (attempt: number) => number
  failureMode?: CoordinationFailureMode
  onEvent?: (event: CrossflightEvent) => void
  onEventError?: (error: unknown) => void
  /**
   * Cache a loader result of `undefined`. The built-in adapters report a
   * stored `undefined` as a miss, so without this a loader that resolves
   * `undefined` runs again for every caller. Crossflight stores a small marked
   * envelope in place of the value and unwraps it on read; values you cached
   * yourself are passed through untouched.
   */
  cacheUndefined?: boolean
}
