import type {
  CacheAdapter,
  CacheLookup,
  CacheSetOptions,
} from '../../src/types.js'

export interface SetCall {
  key: string
  value: unknown
  options?: CacheSetOptions
}

/**
 * A cache adapter that records what it was asked to do and lets a test seed the
 * underlying store directly, which is how a value written by an older version -
 * or by a process with a different `cacheUndefined` setting - is modelled.
 */
export interface StoreHarness {
  /** The raw adapter, without any Crossflight wrapping. */
  readonly adapter: CacheAdapter
  /** Every `set` the adapter received, in order, before any serialization. */
  readonly writes: SetCall[]
  /** Every key the adapter was asked for, in order. */
  readonly reads: string[]
  seed(key: string, value: unknown): void
  /** Drops a key the way an eviction or an external writer would. */
  forget(key: string): void
  peek(key: string): unknown
  has(key: string): boolean
}

function recordingAdapter(handlers: {
  get: (key: string) => CacheLookup<unknown>
  set: (key: string, value: unknown, options?: CacheSetOptions) => void
}): {
  adapter: CacheAdapter
  writes: SetCall[]
  reads: string[]
} {
  const writes: SetCall[] = []
  const reads: string[] = []

  return {
    writes,
    reads,
    adapter: {
      async get<T>(key: string): Promise<CacheLookup<T>> {
        reads.push(key)
        return handlers.get(key) as CacheLookup<T>
      },
      async set<T>(
        key: string,
        value: T,
        options?: CacheSetOptions
      ): Promise<void> {
        writes.push({ key, value, options })
        handlers.set(key, value, options)
      },
    },
  }
}

/**
 * Stores values by reference, so a value of any shape survives and the caller
 * can still tell whether the adapter handed back the very same object. This is
 * the model of a custom adapter that keeps objects in memory.
 */
export function memoryStore(): StoreHarness {
  const values = new Map<string, unknown>()
  const { adapter, writes, reads } = recordingAdapter({
    get: (key) =>
      values.has(key) ? { hit: true, value: values.get(key) } : { hit: false },
    set: (key, value) => {
      values.set(key, value)
    },
  })

  return {
    adapter,
    writes,
    reads,
    seed: (key, value) => values.set(key, value),
    forget: (key) => {
      values.delete(key)
    },
    peek: (key) => values.get(key),
    has: (key) => values.has(key),
  }
}

/**
 * Models the built-in adapters: the value is JSON-serialized on the way in and
 * deserialized on the way out, and a stored `undefined` reads back as a miss -
 * the behaviour `CrossflightOptions.cacheUndefined` exists to work around.
 */
export function jsonStore(): StoreHarness {
  const serialized = new Map<string, string | undefined>()
  const { adapter, writes, reads } = recordingAdapter({
    get: (key) => {
      const raw = serialized.get(key)
      // A serialized `undefined`, or nothing at all: the adapter reports a miss.
      if (typeof raw !== 'string') {
        return { hit: false }
      }

      return { hit: true, value: JSON.parse(raw) }
    },
    set: (key, value) => {
      serialized.set(key, JSON.stringify(value))
    },
  })

  return {
    adapter,
    writes,
    reads,
    seed: (key, value) => serialized.set(key, JSON.stringify(value)),
    forget: (key) => {
      serialized.delete(key)
    },
    peek: (key) => serialized.get(key),
    has: (key) => typeof serialized.get(key) === 'string',
  }
}

/** What a JSON round trip does to a value: what a test should expect back. */
export function normalizeThroughJson(value: unknown): unknown {
  const raw = JSON.stringify(value)

  return raw === undefined ? undefined : JSON.parse(raw)
}
