/**
 * Runs cleanup without letting it hold anything that waits on it, and without
 * leaving a rejection nobody observes unhandled.
 */
export const drain = (work: Promise<unknown>): void => {
  void work.catch(() => undefined)
}

/**
 * Resolves or rejects with the work, but stops waiting the moment the signal
 * aborts and rejects with its abort reason instead. The work is not cancelled
 * here - that is what the signal handed to the loader is for - so a late
 * settlement is drained rather than left to surface as unhandled, unless
 * `onLate` takes it over: a lease acquired after the callers left still owns
 * the key and has to be released.
 */
export const raceWithAbort = <T>(
  work: Promise<T>,
  signal: AbortSignal,
  onLate?: (value: T) => void
): Promise<T> => {
  // Handed over whatever settles first, so a lease that arrives after the
  // abort - even one the coordinator was still working on when the signal was
  // already aborted - is released instead of leaking until its TTL.
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
