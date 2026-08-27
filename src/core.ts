import type {
  CoordinationFailureMode,
  Crossflight,
  CrossflightOptions,
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
  const localFlights = new Map<string, Promise<unknown>>()
  const activeControllers = new Map<string, AbortController>()

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

  const runWithFlight = async <T>(
    key: string,
    loader: () => Promise<T> | T,
    options: WrapOptions = {}
  ): Promise<T> => {
    const current = localFlights.get(key)
    if (current) {
      emit({ type: 'local_join', key })
      return (await current) as T
    }

    const effectiveFailureMode: CoordinationFailureMode =
      options.failureMode ?? failureMode
    const leaseTtlMs = options.ttl ?? defaultTtlMs

    const controller = new AbortController()
    const timeoutMs = options.timeoutMs ?? defaultTimeoutMs
    const signal = options.signal

    if (signal) {
      if (signal.aborted) {
        controller.abort(signal.reason)
      } else {
        signal.addEventListener('abort', () => controller.abort(signal.reason), {
          once: true,
        })
      }
    }

    if (timeoutMs !== undefined && timeoutMs > 0) {
      const timeoutId = setTimeout(() => {
        controller.abort(new CoordinationTimeoutError(key, timeoutMs))
      }, timeoutMs)

      controller.signal.addEventListener('abort', () => clearTimeout(timeoutId), {
        once: true,
      })
    }

    activeControllers.set(key, controller)

    const acquireLease = async () => {
      try {
        return await coordinator.acquire(key, {
          signal: controller.signal,
          ttlMs: leaseTtlMs,
        })
      } catch (error) {
        emit({ type: 'failed', key, error })
        if (controller.signal.aborted) {
          throw controller.signal.reason
        }
        if (effectiveFailureMode === 'fail-open') {
          return null
        }
        throw error
      }
    }

    const flight = (async (): Promise<T> => {
      const startedAt = Date.now()

      try {
        const cached = await cache.get<T>(key)
        if (cached.hit) {
          emit({ type: 'hit', key })
          return cached.value
        }

        emit({ type: 'miss', key })

        let lease = await acquireLease()

        if (!lease && effectiveFailureMode === 'fail-open') {
          return await loader()
        }

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
              emit({ type: 'failed', key, error })
              if (controller.signal.aborted) {
                throw controller.signal.reason
              }
              if (effectiveFailureMode === 'fail-open') {
                return await loader()
              }
              throw error
            }

            attempt += 1

            const retry = await cache.get<T>(key)
            if (retry.hit) {
              emit({ type: 'hit', key })
              return retry.value
            }

            lease = await acquireLease()
            if (lease) {
              break
            }
          }

          if (!lease) {
            const timeoutError = new CoordinationTimeoutError(key)

            emit({ type: 'failed', key, error: timeoutError })

            throw timeoutError
          }
        }

        emit({ type: 'ownership_acquired', key })

        try {
          const recheck = await cache.get<T>(key)
          if (recheck.hit) {
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
                  renewalError = error
                  controller.abort(error)
                  return
                }

                scheduleRenewal()
              })()
            }, renewIntervalMs)
          }

          scheduleRenewal()

          let value: T
          try {
            value = await loader()
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
            // Remove from localFlights before retrying so the recursive call
            // does not join its own outer flight and deadlock.
            localFlights.delete(key)
            return runWithFlight(key, loader, { ...options, signal: controller.signal })
          }

          if (controller.signal.aborted) {
            throw controller.signal.reason
          }

          await cache.set(key, value, { ttl: options.ttl })
          await lease.complete()
          emit({ type: 'completed', key, durationMs: Date.now() - startedAt })
          return value
        } catch (error) {
          emit({ type: 'failed', key, error })
          await lease.abandon().catch(() => undefined)
          throw error
        }
      } catch (error) {
        emit({ type: 'failed', key, error })
        throw error
      } finally {
        localFlights.delete(key)
        activeControllers.delete(key)
      }
    })()

    localFlights.set(key, flight)
    return await flight
  }

  return {
    wrap: async <T>(
      key: string,
      loader: () => Promise<T> | T,
      options?: WrapOptions
    ): Promise<T> => {
      return runWithFlight(key, loader, options ?? {})
    },
    close: async (): Promise<void> => {
      for (const controller of activeControllers.values()) {
        controller.abort(new CoordinationClosedError())
      }
      await coordinator.close()
    },
  }
}
