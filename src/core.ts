import type {
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

interface Flight {
  /** The shared in-flight work for a key; every local caller adopts it. */
  promise: Promise<unknown>
  /** Aborts the shared work: close(), a lost lease, or the last caller's cancel. */
  controller: AbortController
  /** Callers currently waiting that have not cancelled. */
  waitingCallers: number
}

/**
 * Distinguishes "another owner holds the lease" (`contended`) from "the
 * coordinator call itself failed" (`failed`): only the latter may trip the
 * fail-open fallback, otherwise ordinary contention would defeat coalescing.
 */
type LeaseAcquire =
  | { kind: 'acquired'; lease: Lease }
  | { kind: 'contended' }
  | { kind: 'failed' }

export function createCrossflight({
  cache,
  coordinator,
  defaultTimeoutMs,
  defaultTtlMs = DEFAULT_TTL_MS,
  maxRetryAttempts = DEFAULT_MAX_RETRY_ATTEMPTS,
  retryBackoff = DEFAULT_RETRY_BACKOFF,
  failureMode = 'fail-closed',
  onEvent,
  onEventError,
}: CrossflightOptions): Crossflight {
  const localFlights = new Map<string, Flight>()

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
   * Resolves or rejects with the work, but stops waiting the moment the flight
   * aborts and rejects with its abort reason instead. The work is not cancelled
   * here - that is what the signal handed to the loader is for - so a late
   * settlement is drained rather than left to surface as unhandled.
   */
  const raceWithAbort = <T>(
    work: Promise<T>,
    signal: AbortSignal
  ): Promise<T> => {
    work.catch(() => undefined)

    return new Promise<T>((resolve, reject) => {
      if (signal.aborted) {
        reject(signal.reason)
        return
      }

      const onAbort = () => reject(signal.reason)
      signal.addEventListener('abort', onAbort, { once: true })

      work.then(
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
    const existing = localFlights.get(key)
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
    const leaseTtlMs = options.ttl ?? defaultTtlMs

    // The shared controller belongs to the flight, not to any single caller.
    // A caller's signal and timeout are applied by attachCaller, so one
    // caller cancelling never aborts the work while other callers still wait.
    const controller = new AbortController()

    const acquireLease = async (): Promise<LeaseAcquire> => {
      try {
        const lease = await coordinator.acquire(key, {
          signal: controller.signal,
          ttlMs: leaseTtlMs,
        })

        return lease ? { kind: 'acquired', lease } : { kind: 'contended' }
      } catch (error) {
        if (controller.signal.aborted) {
          throw controller.signal.reason
        }
        if (effectiveFailureMode === 'fail-open') {
          // The error does not propagate, so report it here; every throw path
          // is reported once by the outer catch.
          emit({ type: 'failed', key, error })
          return { kind: 'failed' }
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

    const flight = (async (): Promise<T> => {
      const startedAt = Date.now()

      const attempt = async (): Promise<T> => {
        const cached = await cache.get<T>(key)
        if (cached.hit) {
          emit({ type: 'hit', key })
          return cached.value
        }

        emit({ type: 'miss', key })

        const acquired = await acquireLease()

        if (acquired.kind === 'failed') {
          // Coordination failed and fail-open is on: this is the only outcome
          // that runs the loader without joining the distributed wait.
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

            try {
              await waitForRetry(key, attempt, controller.signal)
            } catch (error) {
              if (controller.signal.aborted) {
                throw controller.signal.reason
              }

              if (effectiveFailureMode === 'fail-open') {
                // The wait itself failed: report it and fall back.
                emit({ type: 'failed', key, error })
                return await runLoader(loader, controller.signal)
              }

              throw error
            }

            attempt += 1

            const retry = await cache.get<T>(key)
            if (retry.hit) {
              emit({ type: 'hit', key })
              return retry.value
            }

            const reacquired = await acquireLease()
            if (reacquired.kind === 'failed') {
              return await runLoader(loader, controller.signal)
            }

            if (reacquired.kind === 'acquired') {
              lease = reacquired.lease
              break
            }
          }

          if (!lease) {
            if (effectiveFailureMode === 'fail-open') {
              // The retry budget ran out while the lease stayed contended:
              // report it and fall back to running the loader.
              const timeoutError = new CoordinationTimeoutError(key)
              emit({ type: 'failed', key, error: timeoutError })
              return await runLoader(loader, controller.signal)
            }

            // Reported once by the outer catch.
            throw new CoordinationTimeoutError(key)
          }
        }

        emit({ type: 'ownership_acquired', key })

        try {
          const recheck = await cache.get<T>(key)
          if (recheck.hit) {
            // Another owner filled the cache while we were acquiring, so this
            // lease protects nothing: release it rather than hold it to its TTL.
            await lease.abandon().catch(() => undefined)
            emit({ type: 'hit', key })
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
              await renewalInFlight.catch(() => undefined)
            }
          }

          const scheduleRenewal = () => {
            if (renewalStopped || controller.signal.aborted) {
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
          try {
            value = await runLoader(loader, controller.signal)
          } finally {
            await stopRenewal()
          }

          if (renewalError) {
            throw renewalError
          }

          const stillOwner = await lease.renew()

          if (!stillOwner) {
            const ownershipLost = new OwnershipLostError(key)
            emit({ type: 'failed', key, error: ownershipLost })
            await lease.abandon().catch(() => undefined)
            // Retry the whole attempt on this flight: the record keeps its
            // caller set and controller, so no caller's timeout leaks into the
            // shared retry.
            return attempt()
          }

          if (controller.signal.aborted) {
            throw controller.signal.reason
          }

          // Ownership is deliberately held until the write settles: a cache
          // write has no signal, so releasing the lease early would let a
          // replacement owner publish a newer value that this late, stale
          // write could then overwrite.
          await cache.set(key, value, { ttl: options.ttl })
          await lease.complete()
          emit({ type: 'completed', key, durationMs: Date.now() - startedAt })
          return value
        } catch (error) {
          await lease.abandon().catch(() => undefined)
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
    flight.catch(() => undefined)

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
      for (const record of localFlights.values()) {
        record.controller.abort(new CoordinationClosedError())
      }
      await coordinator.close()
    },
  }
}
