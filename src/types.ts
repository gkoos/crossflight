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
  /** Whole-flight deadline for this call; defaults to `defaultFlightDeadlineMs`. */
  flightDeadlineMs?: number
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
  /**
   * Aborts every in-flight flight and closes the coordinator. The instance is
   * marked closed before the flights are aborted, so a `wrap()` that lands
   * while the coordinator is still shutting down rejects as well.
   *
   * A closed instance rejects every later `wrap()` with
   * `CoordinationClosedError`: it serves no cache hit and runs no loader, in
   * either failure mode, because ownership can no longer be coordinated.
   * Closing is idempotent.
   */
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
  /**
   * Whole-flight deadline in ms, measured from the moment the flight is
   * created. When it passes the flight is aborted: callers waiting on it reject
   * with `CoordinationTimeoutError`, the owner abandons its lease, and a caller
   * that arrives afterwards is rejected at once instead of starting a new
   * flight. The call that creates a flight decides the budget its joiners
   * inherit, and it applies in every failure mode - a `fail-open` fallback
   * could not run on an already aborted signal anyway. Reads are raced with the
   * flight's signal, so a stalled read is bounded as well; the cache write is
   * the one step that cannot be interrupted and is allowed to finish.
   *
   * Unset means the flight has no deadline and each caller's own `timeoutMs` is
   * the only bound.
   */
  defaultFlightDeadlineMs?: number
  onEvent?: (event: CrossflightEvent) => void
  onEventError?: (error: unknown) => void
  /**
   * Cache a loader result of `undefined`. The built-in adapters report a
   * stored `undefined` as a miss, so without this a loader that resolves
   * `undefined` runs again for every caller.
   *
   * While enabled, every value Crossflight writes goes into the reserved
   * envelope `{ "__crossflight_envelope__": 1, value }` and is unwrapped on
   * read, so a loader value can be anything - including an object that looks
   * like the envelope - and still come back exactly as it was. Only a stored
   * value of exactly that shape is unwrapped, so an object that carries the
   * reserved name next to fields of its own is returned as it was stored.
   *
   * Shape is the only discriminator there is: a raw value of exactly the
   * reserved shape - written before this option was turned on, or by a
   * process that has it off - is indistinguishable from an envelope. Migrate
   * or namespace the keys of an existing cache before enabling this, and have
   * every process sharing that cache agree on the setting.
   */
  cacheUndefined?: boolean
}
