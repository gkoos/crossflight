import type {
  CacheAdapter,
  CacheLookup,
  CacheSetOptions,
  CoordinationFailureMode,
  Crossflight,
  CrossflightOptions,
  Lease,
  Loader,
  WrapOptions,
} from './types.js'
import {
  CoordinationClosedError,
  CoordinationTimeoutError,
  OwnershipLostError,
} from './errors.js'

const DEFAULT_TTL_MS = 30_000
const DEFAULT_MAX_RETRY_ATTEMPTS = 64
const DEFAULT_RETRY_BACKOFF = (attempt: number): number =>
  Math.min(200, 25 + attempt * 25 + Math.floor(Math.random() * 25))
const MIN_RENEW_INTERVAL_MS = 25
/**
 * A lease must outlive at least one renewal interval, otherwise it can expire
 * before the first renewal fires and the owner loses ownership mid-load.
 */
const MIN_LEASE_TTL_MS = 2 * MIN_RENEW_INTERVAL_MS

/**
 * Envelope Crossflight writes when `cacheUndefined` is on. Every value is
 * wrapped, so a loader value can be anything - including an object that
 * looks like the envelope itself - and still come back exactly as it was:
 * only the outer layer is ever produced by Crossflight, which is what makes
 * the format unambiguous.
 */
const ENVELOPE_MARKER = '__crossflight_envelope__'
const ENVELOPE_VERSION = 1

interface Envelope {
  [ENVELOPE_MARKER]: typeof ENVELOPE_VERSION
  value?: unknown
}

function isEnvelope(value: unknown): value is Envelope {
  return (
    typeof value === 'object' &&
    value !== null &&
    (value as Record<string, unknown>)[ENVELOPE_MARKER] === ENVELOPE_VERSION
  )
}

/**
 * Lets a cached `undefined` survive the round trip: every value written while
 * the option is on goes into the reserved envelope, and reads unwrap it.
 * Wrapping everything is what keeps the format unambiguous - a loader value
 * that happens to look like the envelope is just a value inside one - while
 * raw values written by a process without the option still read back as they
 * were.
 */
function withCachedUndefined(cache: CacheAdapter): CacheAdapter {
  return {
    async get<T>(key: string): Promise<CacheLookup<T>> {
      const lookup = await cache.get<Envelope | T>(key)
      if (!lookup.hit) {
        return { hit: false }
      }

      if (isEnvelope(lookup.value)) {
        // No `value` key means the loader resolved `undefined`.
        const unwrapped =
          'value' in lookup.value ? lookup.value.value : undefined
        return { hit: true, value: unwrapped as T }
      }

      return { hit: true, value: lookup.value as T }
    },
    async set<T>(
      key: string,
      value: T,
      options?: CacheSetOptions
    ): Promise<void> {
      const envelope: Envelope =
        value === undefined
          ? { [ENVELOPE_MARKER]: ENVELOPE_VERSION }
          : { [ENVELOPE_MARKER]: ENVELOPE_VERSION, value }

      await cache.set(key, envelope as unknown as T, options)
    },
  }
}

interface Flight {
  /** The shared in-flight work for a key; every local caller adopts it. */
  promise: Promise<unknown>
  /** Aborts the shared work: close(), a lost lease, or the last caller's cancel. */
  controller: AbortController
  /** Callers currently waiting that have not cancelled. */
  waitingCallers: number
  /** Absolute time the whole flight gives up, or undefined for no deadline. */
  deadlineAt?: number
}

/**
 * Distinguishes "another owner holds the lease" (`contended`) from "the
 * coordinator call itself failed" (`failed`): only the latter may trip the
 * fail-open fallback, otherwise ordinary contention would defeat coalescing.
 */
type LeaseAcquire =
  | { kind: 'acquired'; lease: Lease }
  | { kind: 'contended' }
  | { kind: 'failed'; error: unknown }

export function createCrossflight({
  cache: providedCache,
  coordinator,
  defaultTimeoutMs,
  defaultTtlMs = DEFAULT_TTL_MS,
  defaultFlightDeadlineMs,
  maxRetryAttempts = DEFAULT_MAX_RETRY_ATTEMPTS,
  retryBackoff = DEFAULT_RETRY_BACKOFF,
  failureMode = 'fail-closed',
  cacheUndefined = false,
  onEvent,
  onEventError,
}: CrossflightOptions): Crossflight {
  const cache = cacheUndefined
    ? withCachedUndefined(providedCache)
    : providedCache
  const localFlights = new Map<string, Flight>()

  /**
   * Set before close() aborts the flights, so a call that lands while the
   * coordinator is still shutting down is rejected rather than starting work
   * that nothing owns any more.
   */
  let closed = false

  const emit = (event: Parameters<NonNullable<typeof onEvent>>[0]) => {
    if (!onEvent) {
      return
    }

    try {
      onEvent(event)
    } catch (error) {
      if (onEventError) {
        try {
          onEventError(error)
        } catch {
          // onEventError itself must never break the library.
        }
      }
    }
  }

  const waitForRetry = async (
    key: string,
    attempt: number,
    signal?: AbortSignal
  ): Promise<void> => {
    const delayMs = retryBackoff(attempt)
    await coordinator.waitForChange(key, {
      signal,
      timeoutMs: delayMs,
    })
  }

  /**
   * Runs cleanup without letting it hold anything that waits on it, and without
   * leaving a rejection nobody observes unhandled.
   */
  const drain = (work: Promise<unknown>): void => {
    void work.catch(() => undefined)
  }

  /**
   * Resolves or rejects with the work, but stops waiting the moment the flight
   * aborts and rejects with its abort reason instead. The work is not cancelled
   * here - that is what the signal handed to the loader is for - so a late
   * settlement is drained rather than left to surface as unhandled, unless
   * `onLate` takes it over: a lease acquired after the callers left still owns
   * the key and has to be released.
   */
  const raceWithAbort = <T>(
    work: Promise<T>,
    signal: AbortSignal,
    onLate?: (value: T) => void
  ): Promise<T> => {
    // Handed over whatever settles first, so a lease that arrives after the
    // abort - even one the coordinator was still working on when the signal
    // was already aborted - is released instead of leaking until its TTL.
    const observed =
      onLate === undefined
        ? work
        : work.then((value) => {
            if (signal.aborted) {
              onLate(value)
            }

            return value
          })

    drain(observed)

    return new Promise<T>((resolve, reject) => {
      if (signal.aborted) {
        reject(signal.reason)
        return
      }

      const onAbort = () => reject(signal.reason)
      signal.addEventListener('abort', onAbort, { once: true })

      observed.then(
        (value) => {
          signal.removeEventListener('abort', onAbort)
          resolve(value)
        },
        (error) => {
          signal.removeEventListener('abort', onAbort)
          reject(error)
        }
      )
    })
  }

  /**
   * Runs the loader with the flight's abort signal and never waits past an
   * abort, so a loader that ignores the signal cannot hold the flight - and
   * with it the lease and the cache write - open. A flight that has already
   * aborted never starts the loader at all.
   */
  const runLoader = <T>(loader: Loader<T>, signal: AbortSignal): Promise<T> =>
    raceWithAbort(
      signal.aborted
        ? Promise.reject<T>(signal.reason)
        : (async () => loader(signal))(),
      signal
    )

  /**
   * Binds one caller to a shared flight. The caller's own signal and timeout
   * only ever cancel that caller's wait: while other callers are still waiting
   * they leave the shared work untouched. Cancelling always settles the caller
   * with its own reason straight away; the shared flight is aborted as well
   * once the last caller leaves, but the caller never waits for a loader that
   * ignores the abort signal to observe it.
   */
  const attachCaller = <T>(
    key: string,
    record: Flight,
    options: WrapOptions
  ): Promise<T> => {
    const signal = options.signal
    const timeoutMs = options.timeoutMs ?? defaultTimeoutMs
    const hasTimeout = timeoutMs !== undefined && timeoutMs > 0

    // Nothing can cancel this caller, so the shared flight is its result.
    if (!signal && !hasTimeout) {
      return record.promise as Promise<T>
    }

    return new Promise<T>((resolve, reject) => {
      let settled = false
      let timeoutId: ReturnType<typeof setTimeout> | undefined
      let detachAbort: (() => void) | undefined

      const cleanup = () => {
        if (timeoutId !== undefined) {
          clearTimeout(timeoutId)
          timeoutId = undefined
        }

        detachAbort?.()
        detachAbort = undefined
      }

      const cancelCaller = (reason: unknown) => {
        if (settled) {
          return
        }

        settled = true
        cleanup()
        record.waitingCallers -= 1
        emit({ type: 'cancelled', key, reason })

        // This caller always settles with its own reason at once; the aborted
        // flight stops waiting for its loader and reports its own failure.
        if (record.waitingCallers === 0) {
          record.controller.abort(reason)
        }

        reject(reason)
      }

      if (timeoutMs !== undefined && timeoutMs > 0) {
        const delayMs = timeoutMs

        timeoutId = setTimeout(() => {
          cancelCaller(new CoordinationTimeoutError(key, delayMs))
        }, delayMs)

        // Do not keep an exiting process alive; the timer still fires while it runs.
        timeoutId.unref()
      }

      if (signal) {
        const onAbort = () => cancelCaller(signal.reason)

        if (signal.aborted) {
          cancelCaller(signal.reason)
          return
        }

        signal.addEventListener('abort', onAbort, { once: true })
        detachAbort = () => signal.removeEventListener('abort', onAbort)
      }

      record.promise.then(
        (value) => {
          if (settled) {
            return
          }

          settled = true
          cleanup()
          resolve(value as T)
        },
        (error) => {
          if (settled) {
            return
          }

          settled = true
          cleanup()
          reject(error)
        }
      )
    })
  }

  const runWithFlight = async <T>(
    key: string,
    loader: Loader<T>,
    options: WrapOptions = {}
  ): Promise<T> => {
    // A closed instance does no work at all: a cache hit is not served and no
    // loader runs, in either failure mode, because ownership can no longer be
    // coordinated. Checked before anything else, so a call that lands while
    // close() is still tearing the coordinator down is rejected too.
    if (closed) {
      throw new CoordinationClosedError()
    }

    const existing = localFlights.get(key)

    // A caller that arrives after the flight's deadline must not start a new
    // flight - that is how a deadline turns into a stampede - and must not join
    // a flight that is already winding down.
    if (
      existing?.deadlineAt !== undefined &&
      Date.now() >= existing.deadlineAt
    ) {
      throw new CoordinationTimeoutError(key)
    }

    // An aborted record is a flight that is winding down; joining it would hand
    // its caller the previous caller's cancellation reason. Start a fresh one
    // instead - the finally identity check keeps the replacement safe.
    if (existing && !existing.controller.signal.aborted) {
      emit({ type: 'local_join', key })
      existing.waitingCallers += 1
      return attachCaller<T>(key, existing, options)
    }

    const effectiveFailureMode: CoordinationFailureMode =
      options.failureMode ?? failureMode
    const leaseTtlMs = Math.max(options.leaseTtlMs ?? defaultTtlMs, MIN_LEASE_TTL_MS)

    // The shared controller belongs to the flight, not to any single caller.
    // A caller's signal and timeout are applied by attachCaller, so one
    // caller cancelling never aborts the work while other callers still wait.
    const controller = new AbortController()

    const acquireLease = async (): Promise<LeaseAcquire> => {
      try {
        const lease = await raceWithAbort(
          coordinator.acquire(key, {
            signal: controller.signal,
            ttlMs: leaseTtlMs,
          }),
          controller.signal,
          // A lease that arrives after the flight gave up still owns the key:
          // release it rather than leak it until its TTL.
          (lateLease) => {
            if (lateLease) {
              drain(lateLease.abandon())
            }
          }
        )

        return lease ? { kind: 'acquired', lease } : { kind: 'contended' }
      } catch (error) {
        if (controller.signal.aborted) {
          throw controller.signal.reason
        }
        if (effectiveFailureMode === 'fail-open') {
          // The error does not propagate, so report it here; every throw path
          // is reported once by the outer catch.
          emit({ type: 'failed', key, error })
          return { kind: 'failed', error }
        }
        throw error
      }
    }

    const record: Flight = {
      // Replaced with the flight promise below, before any caller can read it.
      promise: Promise.resolve(),
      controller,
      waitingCallers: 1,
    }

    const flightDeadlineMs = options.flightDeadlineMs ?? defaultFlightDeadlineMs
    let deadlineTimer: ReturnType<typeof setTimeout> | undefined

    if (flightDeadlineMs !== undefined && flightDeadlineMs > 0) {
      record.deadlineAt = Date.now() + flightDeadlineMs
      // The deadline is a latency budget for the whole flight rather than a
      // coordination failure, so it aborts in every failure mode: a fail-open
      // fallback could not run on an already aborted signal anyway.
      deadlineTimer = setTimeout(() => {
        record.controller.abort(new CoordinationTimeoutError(key))
      }, flightDeadlineMs)
      // Do not keep an exiting process alive; the timer still fires while it runs.
      deadlineTimer.unref()
    }

    const flight = (async (): Promise<T> => {
      const startedAt = Date.now()

      // Accumulated distributed-wait time: cache reads, acquire probes and a
      // previous attempt of this flight (after ownership loss) are not waits on
      // another owner, so they stay out of the reported metric.
      let waitedMs = 0

      // A cache read carries no signal of its own, so race it with the flight's:
      // a stalled read must not hold the flight past its deadline. Losing the
      // race is safe - a read has no side effects and is drained, not cancelled.
      const readCache = (): Promise<CacheLookup<T>> =>
        raceWithAbort(cache.get<T>(key), controller.signal)

      const attempt = async (): Promise<T> => {
        const cached = await readCache()
        if (cached.hit) {
          emit({ type: 'hit', key, waitedMs: 0 })
          return cached.value
        }

        emit({ type: 'miss', key })

        const acquired = await acquireLease()

        if (acquired.kind === 'failed') {
          // Coordination failed and fail-open is on: this is the only outcome
          // that runs the loader without joining the distributed wait.
          emit({ type: 'fallback', key, reason: acquired.error })
          return await runLoader(loader, controller.signal)
        }

        let lease = acquired.kind === 'acquired' ? acquired.lease : null

        if (!lease) {
          emit({ type: 'distributed_join', key })
          let attempt = 0

          while (attempt < maxRetryAttempts) {
            if (controller.signal.aborted) {
              throw controller.signal.reason
            }

            const waitStartedAt = Date.now()

            try {
              await raceWithAbort(
                waitForRetry(key, attempt, controller.signal),
                controller.signal
              )
            } catch (error) {
              waitedMs += Date.now() - waitStartedAt

              if (controller.signal.aborted) {
                throw controller.signal.reason
              }

              if (effectiveFailureMode === 'fail-open') {
                // The wait itself failed: report it and fall back.
                emit({ type: 'failed', key, error })
                emit({ type: 'fallback', key, reason: error })
                return await runLoader(loader, controller.signal)
              }

              throw error
            }

            waitedMs += Date.now() - waitStartedAt

            attempt += 1

            const retry = await readCache()
            if (retry.hit) {
              emit({ type: 'hit', key, waitedMs })
              return retry.value
            }

            const reacquired = await acquireLease()
            if (reacquired.kind === 'failed') {
              emit({ type: 'fallback', key, reason: reacquired.error })
              return await runLoader(loader, controller.signal)
            }

            if (reacquired.kind === 'acquired') {
              lease = reacquired.lease
              break
            }
          }

          if (!lease) {
            // The retry budget ran out while the lease stayed contended.
            emit({ type: 'wait_exhausted', key, attempts: maxRetryAttempts })

            if (effectiveFailureMode === 'fail-open') {
              // Report it and fall back to running the loader.
              const timeoutError = new CoordinationTimeoutError(key)
              emit({ type: 'failed', key, error: timeoutError })
              emit({ type: 'fallback', key, reason: timeoutError })
              return await runLoader(loader, controller.signal)
            }

            // Reported once by the outer catch.
            throw new CoordinationTimeoutError(key)
          }
        }

        emit({ type: 'ownership_acquired', key })

        try {
          const recheck = await readCache()
          if (recheck.hit) {
            // Another owner filled the cache while we were acquiring, so this
            // lease protects nothing: release it rather than hold it to its TTL.
            drain(lease.abandon())
            emit({ type: 'hit', key, waitedMs })
            return recheck.value
          }

          let renewalError: unknown | null = null
          let renewalTimer: ReturnType<typeof setTimeout> | null = null
          let renewalInFlight: Promise<void> | null = null
          let renewalStopped = false
          const renewIntervalMs = Math.max(
            MIN_RENEW_INTERVAL_MS,
            Math.floor(leaseTtlMs / 2)
          )

          const stopRenewal = async () => {
            renewalStopped = true
            if (renewalTimer) {
              clearTimeout(renewalTimer)
              renewalTimer = null
            }

            if (renewalInFlight) {
              await raceWithAbort(renewalInFlight, controller.signal).catch(
                () => undefined
              )
            }
          }

          const scheduleRenewal = () => {
            // Only an explicit stop ends renewal: an abort must not, because a
            // cache write that is already in flight keeps running and its lease
            // has to stay held until it settles.
            if (renewalStopped) {
              return
            }

            renewalTimer = setTimeout(() => {
              renewalInFlight = (async () => {
                try {
                  const stillOwner = await lease.renew()
                  if (!stillOwner) {
                    renewalError = new OwnershipLostError(key)
                    controller.abort(renewalError)
                    return
                  }
                } catch (error) {
                  emit({ type: 'renewal_failed', key, error })
                  renewalError = error
                  controller.abort(error)
                  return
                }

                scheduleRenewal()
              })()
            }, renewIntervalMs)

            // Do not keep an exiting process alive; the timer still fires while it runs.
            renewalTimer.unref()
          }

          scheduleRenewal()

          let value: T

          // Renewal stays active until the publication settles. A cache write
          // has no signal and can outlive its lease, and a lease that expires
          // mid-write lets a replacement owner publish a newer value that this
          // late, stale write would then overwrite. Holding the lease through
          // the write is what makes the ownership guarantee real; if a renewal
          // still fails mid-write, ownership is gone for good and only a store
          // that rejects stale writes could prevent the overwrite (see the
          // README's failure semantics).
          try {
            value = await runLoader(loader, controller.signal)

            // Raced like every other coordination await: a renewal the
            // coordinator never answers must not outlive the deadline.
            const stillOwner = await raceWithAbort(
              lease.renew(),
              controller.signal
            )

            if (!stillOwner) {
              const ownershipLost = new OwnershipLostError(key)
              emit({ type: 'failed', key, error: ownershipLost })
              drain(lease.abandon())
              // Retry the whole attempt on this flight: the record keeps its
              // caller set and controller, so no caller's timeout leaks into
              // the shared retry.
              return attempt()
            }

            // The renewal above is raced with the flight signal, so an abort
            // while it is in flight already stops the publication; the cache
            // write itself is the one step that cannot be interrupted.

            await cache.set(key, value, { ttl: options.ttl })

            // Drain renewal before releasing the lease: a renewal that is
            // still in flight would otherwise observe the completed lease,
            // report ownership lost, and turn a successful publication into a
            // failed one.
            await stopRenewal()
            drain(lease.complete())
          } finally {
            await stopRenewal()
          }

          if (renewalError) {
            // Ownership was lost while the value was being published: the
            // write has already happened, so the caller still receives it, but
            // the loss is reported like any other ownership loss.
            emit({ type: 'failed', key, error: renewalError })
          }

          emit({
            type: 'completed',
            key,
            durationMs: Date.now() - startedAt,
            waitedMs,
          })
          return value
        } catch (error) {
          drain(lease.abandon())
          // Reported once by the outer catch.
          throw error
        }
      }

      try {
        return await attempt()
      } catch (error) {
        emit({ type: 'failed', key, error })
        throw error
      } finally {
        if (deadlineTimer) {
          clearTimeout(deadlineTimer)
        }

        // Only remove our own record: a caller that found it aborted may have
        // replaced it with a fresh flight for the same key.
        if (localFlights.get(key) === record) {
          localFlights.delete(key)
        }
      }
    })()

    record.promise = flight

    // If every waiting caller detaches before the flight settles, the abort is
    // observed here so a late rejection never surfaces as unhandled.
    drain(flight)

    localFlights.set(key, record)

    return attachCaller<T>(key, record, options)
  }

  return {
    wrap: async <T>(
      key: string,
      loader: Loader<T>,
      options?: WrapOptions
    ): Promise<T> => {
      return runWithFlight(key, loader, options ?? {})
    },
    close: async (): Promise<void> => {
      // Marked closed first: work started while the coordinator is still
      // shutting down would be unowned, so a call racing this one has to reject
      // instead of falling through to the cache or a fail-open loader.
      closed = true

      for (const record of localFlights.values()) {
        record.controller.abort(new CoordinationClosedError())
      }
      await coordinator.close()
    },
  }
}
