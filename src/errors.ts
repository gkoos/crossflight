export class CoordinationError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options)
    this.name = 'CoordinationError'
  }
}

export class CoordinationClosedError extends CoordinationError {
  constructor(message = 'Crossflight has been closed') {
    super(message)
    this.name = 'CoordinationClosedError'
  }
}

export class CoordinationTimeoutError extends CoordinationError {
  readonly key: string

  constructor(key: string, durationMs?: number) {
    const message = durationMs !== undefined
      ? `Operation timed out after ${durationMs}ms for key "${key}"`
      : `Timed out waiting for distributed coalescing to complete for key "${key}"`
    super(message)
    this.name = 'CoordinationTimeoutError'
    this.key = key
  }
}

export class OwnershipLostError extends CoordinationError {
  readonly key: string

  constructor(key: string) {
    super(`Ownership of key "${key}" was lost before the value could be published`)
    this.name = 'OwnershipLostError'
    this.key = key
  }
}
