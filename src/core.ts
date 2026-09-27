import type {
  CoordinationFailureMode,
  Crossflight,
  CrossflightOptions,
  Loader,
  WrapOptions,
} from './types.js'
import { CoordinationClosedError, CoordinationTimeoutError } from './errors.js'
import { withCachedUndefined } from './envelope.js'
import { drain } from './async-utils.js'
import { MIN_LEASE_TTL_MS } from './lease-keeper.js'
import { runFlight, type Flight } from './flight.js'
import { attachCaller } from './caller.js'

const DEFAULT_TTL_MS = 30_000
const DEFAULT_MAX_RETRY_ATTEMPTS = 64
const DEFAULT_RETRY_BACKOFF = (attempt: number): number =>
  Math.min(200, 25 + attempt * 25 + Math.floor(Math.random() * 25))

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

  // Set before close() aborts flights, so a call landing during teardown rejects.
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

  const runWithFlight = async <T>(
    key: string,
    loader: Loader<T>,
    options: WrapOptions = {}
  ): Promise<T> => {
    // A closed instance serves no hit and runs no loader: ownership can no
    // longer be coordinated, so the call rejects before anything else.
    if (closed) {
      throw new CoordinationClosedError()
    }

    const existing = localFlights.get(key)

    // A caller arriving after the deadline must not start a new flight (that is
    // how a deadline turns into a stampede) and must not join a winding-down one.
    if (
      existing?.deadlineAt !== undefined &&
      Date.now() >= existing.deadlineAt
    ) {
      throw new CoordinationTimeoutError(key)
    }

    // An aborted record is winding down; joining would hand this caller the
    // previous caller's cancellation reason. Start fresh - the finally identity
    // check keeps the replacement safe.
    if (existing && !existing.controller.signal.aborted) {
      emit({ type: 'local_join', key })
      existing.waitingCallers += 1
      return attachCaller<T>(key, existing, options, { defaultTimeoutMs, emit })
    }

    const effectiveFailureMode: CoordinationFailureMode =
      options.failureMode ?? failureMode
    const leaseTtlMs = Math.max(
      options.leaseTtlMs ?? defaultTtlMs,
      MIN_LEASE_TTL_MS
    )

    // The controller belongs to the flight, not any one caller.
    const controller = new AbortController()

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
      deadlineTimer = setTimeout(() => {
        record.controller.abort(new CoordinationTimeoutError(key))
      }, flightDeadlineMs)
      deadlineTimer.unref()
    }

    const flight = runFlight<T>({
      key,
      loader,
      options,
      effectiveFailureMode,
      leaseTtlMs,
      maxRetryAttempts,
      retryBackoff,
      controller,
      record,
      deadlineTimer,
      localFlights,
      cache,
      coordinator,
      emit,
    })

    record.promise = flight
    // If every caller detaches before settle, the abort is observed here so a
    // late rejection never surfaces unhandled.
    drain(flight)
    localFlights.set(key, record)

    return attachCaller<T>(key, record, options, { defaultTimeoutMs, emit })
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
      // Marked closed first: work started mid-teardown would be unowned.
      closed = true

      for (const record of localFlights.values()) {
        record.controller.abort(new CoordinationClosedError())
      }
      await coordinator.close()
    },
  }
}
