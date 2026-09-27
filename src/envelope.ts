import type { CacheAdapter, CacheLookup, CacheSetOptions } from './types.js'

const ENVELOPE_MARKER = '__crossflight_envelope__'
const ENVELOPE_VERSION = 1

interface Envelope {
  [ENVELOPE_MARKER]: typeof ENVELOPE_VERSION
  value?: unknown
}

/**
 * Only the shape Crossflight writes counts as an envelope: the marker as an own
 * key, and nothing beside it but `value`. A stored object that carries the
 * reserved name next to fields of its own is application data that happens to
 * use it, and unwrapping that would hand the caller a `value` - or an
 * `undefined` - that was never written under the key.
 *
 * Shape is the only discriminator there is, so a raw value that is exactly an
 * envelope cannot be told apart from one Crossflight wrote; see
 * `CrossflightOptions.cacheUndefined`.
 */
function isEnvelope(value: unknown): value is Envelope {
  if (
    typeof value !== 'object' ||
    value === null ||
    (value as Record<string, unknown>)[ENVELOPE_MARKER] !== ENVELOPE_VERSION
  ) {
    return false
  }

  const keys = Object.keys(value)

  return (
    keys.includes(ENVELOPE_MARKER) &&
    keys.every((key) => key === ENVELOPE_MARKER || key === 'value')
  )
}

/**
 * Lets a cached `undefined` survive the round trip: every value written while
 * the option is on goes into the reserved envelope, and reads unwrap it.
 * Wrapping everything is what keeps Crossflight's own values unambiguous - a
 * loader value that happens to look like the envelope is just a value inside
 * one - while a raw value that is not exactly the envelope shape still reads
 * back as it was.
 */
export function withCachedUndefined(cache: CacheAdapter): CacheAdapter {
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
