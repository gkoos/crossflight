import type { Lease } from './types.js'

export const MIN_RENEW_INTERVAL_MS = 25

/**
 * A lease must outlive at least one renewal interval, otherwise it can expire
 * before the first renewal fires and the owner loses ownership mid-load.
 */
export const MIN_LEASE_TTL_MS = 2 * MIN_RENEW_INTERVAL_MS

export interface LeaseKeeperOptions {
  lease: Lease
  renewIntervalMs: number
  /** Races a promise against the flight signal; used to drain an in-flight renewal. */
  raceWithAbort: <T>(work: Promise<T>, signal: AbortSignal) => Promise<T>
  signal: AbortSignal
  /** Renewal returned false: the lease has been taken over. */
  onOwnershipLost: () => void
  /** Renewal itself failed (coordinator error). */
  onRenewError: (error: unknown) => void
}

/**
 * Owns the lease-renewal loop: it keeps the lease alive while the owner loads
 * and publishes, and stops cleanly before the lease is released. Renewals are
 * chained rather than overlapped - the next timer starts only after the current
 * renewal settles - so a slow coordinator backs off instead of piling up.
 */
export class LeaseKeeper {
  private timer: ReturnType<typeof setTimeout> | null = null
  private inFlight: Promise<void> | null = null
  private stopped = false

  constructor(private readonly options: LeaseKeeperOptions) {}

  start(): void {
    this.schedule()
  }

  async stop(): Promise<void> {
    this.stopped = true
    if (this.timer !== null) {
      clearTimeout(this.timer)
      this.timer = null
    }

    if (this.inFlight !== null) {
      await this.options
        .raceWithAbort(this.inFlight, this.options.signal)
        .catch(() => undefined)
    }
  }

  private schedule(): void {
    // Only an explicit stop ends renewal: an abort must not, because a cache
    // write that is already in flight keeps running and its lease has to stay
    // held until it settles.
    if (this.stopped) {
      return
    }

    const timer = setTimeout(() => {
      this.inFlight = (async () => {
        try {
          const stillOwner = await this.options.lease.renew()
          if (!stillOwner) {
            this.options.onOwnershipLost()
            return
          }
        } catch (error) {
          this.options.onRenewError(error)
          return
        }

        this.schedule()
      })()
    }, this.options.renewIntervalMs)

    // Do not keep an exiting process alive; the timer still fires while it runs.
    timer.unref()
    this.timer = timer
  }
}
