import type {
  CacheAdapter,
  CacheLookup,
  CoordinationFailureMode,
  Coordinator,
  CrossflightEvent,
  Lease,
  Loader,
  WrapOptions,
} from './types.js'
import { CoordinationTimeoutError, OwnershipLostError } from './errors.js'
import { drain, raceWithAbort } from './async-utils.js'
import { LeaseKeeper, MIN_RENEW_INTERVAL_MS } from './lease-keeper.js'

export interface Flight {
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
export type LeaseAcquire =
  | { kind: 'acquired'; lease: Lease }
  | { kind: 'contended' }
  | { kind: 'failed'; error: unknown }

export interface RunFlightDeps<T> {
  key: string
  loader: Loader<T>
  options: WrapOptions
  effectiveFailureMode: CoordinationFailureMode
  leaseTtlMs: number
  maxRetryAttempts: number
  retryBackoff: (attempt: number) => number

  controller: AbortController
  record: Flight
  deadlineTimer: ReturnType<typeof setTimeout> | undefined
  localFlights: Map<string, Flight>

  cache: CacheAdapter
  coordinator: Coordinator

  emit: (event: CrossflightEvent) => void
}

/**
 * Runs one flight for a key to completion: the cache read, the distributed
 * acquire/contend state machine, and - for the owner - the loader and the
 * cache publication with the lease held through the write. Ownership of the
 * record's lifecycle (deadline timer cleanup and removal from `localFlights`)
 * lives here so the composition root only has to wire the pieces together.
 */
export function runFlight<T>(deps: RunFlightDeps<T>): Promise<T> {
  const {
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
  } = deps

  const startedAt = Date.now()

  // Accumulated distributed-wait time: cache reads, acquire probes and a
  // previous attempt of this flight (after ownership loss) are not waits on
  // another owner, so they stay out of the reported metric.
  let waitedMs = 0

  // A cache read carries no signal of its own, so race it with the flight's:
  // a stalled read must not hold the flight past its deadline. Losing the race
  // is safe - a read has no side effects and is drained, not cancelled.
  const readCache = (): Promise<CacheLookup<T>> =>
    raceWithAbort(cache.get<T>(key), controller.signal)

  // Runs the loader with the flight's abort signal and never waits past an
  // abort, so a loader that ignores the signal cannot hold the flight - and
  // with it the lease and the cache write - open. A flight that has already
  // aborted never starts the loader at all.
  const runLoader = (signal: AbortSignal): Promise<T> =>
    raceWithAbort(
      signal.aborted
        ? Promise.reject<T>(signal.reason)
        : (async () => loader(signal))(),
      signal
    )

  const waitForRetry = async (attempt: number): Promise<void> => {
    const delayMs = retryBackoff(attempt)
    await coordinator.waitForChange(key, {
      signal: controller.signal,
      timeoutMs: delayMs,
    })
  }

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

  const attempt = async (): Promise<T> => {
    const cached = await readCache()
    if (cached.hit) {
      emit({ type: 'hit', key, waitedMs: 0 })
      return cached.value
    }

    emit({ type: 'miss', key })

    const acquired = await acquireLease()

    if (acquired.kind === 'failed') {
      // Coordination failed and fail-open is on: this is the only outcome that
      // runs the loader without joining the distributed wait.
      emit({ type: 'fallback', key, reason: acquired.error })
      return await runLoader(controller.signal)
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
          await raceWithAbort(waitForRetry(attempt), controller.signal)
        } catch (error) {
          waitedMs += Date.now() - waitStartedAt

          if (controller.signal.aborted) {
            throw controller.signal.reason
          }

          if (effectiveFailureMode === 'fail-open') {
            // The wait itself failed: report it and fall back.
            emit({ type: 'failed', key, error })
            emit({ type: 'fallback', key, reason: error })
            return await runLoader(controller.signal)
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
          return await runLoader(controller.signal)
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
          const timeoutError = new CoordinationTimeoutError(key)
          emit({ type: 'failed', key, error: timeoutError })
          emit({ type: 'fallback', key, reason: timeoutError })
          return await runLoader(controller.signal)
        }

        // Reported once by the outer catch.
        throw new CoordinationTimeoutError(key)
      }
    }

    emit({ type: 'ownership_acquired', key })

    try {
      const recheck = await readCache()
      if (recheck.hit) {
        // Another owner filled the cache while we were acquiring, so this lease
        // protects nothing: release it rather than hold it to its TTL.
        drain(lease.abandon())
        emit({ type: 'hit', key, waitedMs })
        return recheck.value
      }

      let renewalError: unknown | null = null
      const renewIntervalMs = Math.max(
        MIN_RENEW_INTERVAL_MS,
        Math.floor(leaseTtlMs / 2)
      )

      // Renewal stays active until the publication settles. A cache write has no
      // signal and can outlive its lease, and a lease that expires mid-write lets
      // a replacement owner publish a newer value that this late, stale write
      // would then overwrite. Holding the lease through the write is what makes
      // the ownership guarantee real; if a renewal still fails mid-write,
      // ownership is gone for good and only a store that rejects stale writes
      // could prevent the overwrite (see the README's failure semantics).
      const keeper = new LeaseKeeper({
        lease,
        renewIntervalMs,
        raceWithAbort,
        signal: controller.signal,
        onOwnershipLost: () => {
          const error = new OwnershipLostError(key)
          renewalError = error
          controller.abort(error)
        },
        onRenewError: (error) => {
          emit({ type: 'renewal_failed', key, error })
          renewalError = error
          controller.abort(error)
        },
      })
      keeper.start()

      let value: T

      try {
        value = await runLoader(controller.signal)

        // Raced like every other coordination await: a renewal the coordinator
        // never answers must not outlive the deadline.
        const stillOwner = await raceWithAbort(lease.renew(), controller.signal)

        if (!stillOwner) {
          const ownershipLost = new OwnershipLostError(key)
          emit({ type: 'failed', key, error: ownershipLost })
          drain(lease.abandon())
          // Retry the whole attempt on this flight: the record keeps its caller
          // set and controller, so no caller's timeout leaks into the shared retry.
          return attempt()
        }

        // The renewal above is raced with the flight signal, so an abort while it
        // is in flight already stops the publication; the cache write itself is
        // the one step that cannot be interrupted.
        await cache.set(key, value, { ttl: options.ttl })

        // Drain renewal before releasing the lease: a renewal that is still in
        // flight would otherwise observe the completed lease, report ownership
        // lost, and turn a successful publication into a failed one.
        await keeper.stop()
        drain(lease.complete())
      } finally {
        await keeper.stop()
      }

      if (renewalError) {
        // Ownership was lost while the value was being published: the write has
        // already happened, so the caller still receives it, but the loss is
        // reported like any other ownership loss.
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

  return (async (): Promise<T> => {
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
}
