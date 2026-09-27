import type { CrossflightEvent, WrapOptions } from './types.js'
import type { Flight } from './flight.js'
import { CoordinationTimeoutError } from './errors.js'

export interface AttachCallerDeps {
  defaultTimeoutMs: number | undefined
  emit: (event: CrossflightEvent) => void
}

/**
 * Binds one caller to a shared flight. The caller's own signal and timeout only
 * ever cancel that caller's wait: while other callers are still waiting they
 * leave the shared work untouched. Cancelling always settles the caller with
 * its own reason straight away; the shared flight is aborted as well once the
 * last caller leaves, but the caller never waits for a loader that ignores the
 * abort signal to observe it.
 */
export const attachCaller = <T>(
  key: string,
  record: Flight,
  options: WrapOptions,
  deps: AttachCallerDeps
): Promise<T> => {
  const signal = options.signal
  const timeoutMs = options.timeoutMs ?? deps.defaultTimeoutMs
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

    // No re-entry guard is needed here: cleanup() removes both triggers before
    // any user code runs, and each of them fires at most once.
    const cancelCaller = (reason: unknown) => {
      settled = true
      cleanup()
      record.waitingCallers -= 1
      deps.emit({ type: 'cancelled', key, reason })

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
