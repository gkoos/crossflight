import { createHash } from 'node:crypto'

import * as fc from 'fast-check'
import { describe, expect, it } from 'vitest'

import { redisCoordinator } from '../../src/coordinators/redis.js'
import { FakeRedisClient } from '../support/fake-redis.js'
import { createPropertySuite } from '../support/seed.js'

/**
 * The Redis coordinator maps a logical key onto real Redis keys, so its layout
 * is not an implementation detail: two processes that disagree about it - an
 * older version, or two tenants with a careless namespace - coordinate through
 * different locks. A collision here is not a slow cache, it is two owners
 * believing they hold the same flight, so the mapping is checked directly.
 */
const itProperty = createPropertySuite('redis-keys', { runs: 80 })

const DEFAULT_NAMESPACE = 'crossflight'

const hashTag = (key: string): string =>
  createHash('sha256').update(key).digest('hex')

const leaseKeyFor = (namespace: string, key: string): string =>
  `${namespace}:{${hashTag(key)}}:flight`

const changeChannelFor = (namespace: string, key: string): string =>
  `${namespace}:{${hashTag(key)}}:change`

interface Observation {
  leaseKey: string
  channel: string
  publishedChannel: string
  subscribedChannel: string | undefined
  token: string
  ttl: string
}

/**
 * Runs one acquire against a recording client and reports the keys, the channel
 * and the script arguments the coordinator derived from the logical key.
 */
async function observe(options: {
  key: string
  namespace?: string
  hashKey?: (key: string) => string
  ttlMs?: number
  subscribe?: boolean
}): Promise<Observation> {
  const command = new FakeRedisClient()
  const coordinator = redisCoordinator(command.asClient(), {
    namespace: options.namespace,
    hashKey: options.hashKey,
  })

  const lease = await coordinator.acquire(options.key, { ttlMs: options.ttlMs })
  expect(lease).not.toBeNull()

  if (options.subscribe) {
    await coordinator.waitForChange(options.key, { timeoutMs: 5 })
  }

  const evalCall = command.evals[0]
  const published = command.published[0]

  expect(evalCall).toBeDefined()
  expect(published).toBeDefined()

  const observation: Observation = {
    leaseKey: evalCall!.keys[0]!,
    channel: published!.channel,
    publishedChannel: published!.channel,
    subscribedChannel: command.duplicates[0]?.subscribed[0],
    token: evalCall!.args[0]!,
    ttl: evalCall!.args[1]!,
  }

  await coordinator.close()

  return observation
}

/**
 * Namespaces and keys that try to talk their way back into the template: braces
 * and colons are what the layout is built from, so they are exactly what a
 * collision would have to be made of.
 */
const namespaces = [
  '',
  ':',
  '{',
  '}',
  '{:',
  '}:',
  DEFAULT_NAMESPACE,
  'a:{b',
  'ns:{deadbeef}:flight',
]

const logicalKeys = [
  '',
  'user:1',
  '{',
  '}',
  '{...}',
  'crossflight:{deadbeef}:flight',
  'a}:flight',
  ':flight',
  '{x}:change',
]

describe('redis coordinator key derivation', () => {
  itProperty(
    'maps every distinct (namespace, key) pair onto a distinct lease key',
    fc.array(
      fc.record({
        namespace: fc.oneof(
          fc.constantFrom(...namespaces),
          fc.string({ maxLength: 16 })
        ),
        key: fc.oneof(
          fc.constantFrom(...logicalKeys),
          fc.string({ maxLength: 16 })
        ),
      }),
      { minLength: 2, maxLength: 5 }
    ),
    async (samples) => {
      const keyByPair = new Map<string, string>()

      for (const sample of samples) {
        const { leaseKey } = await observe(sample)
        // JSON encodes the pair unambiguously, whatever the strings contain.
        const pair = JSON.stringify([sample.namespace, sample.key])
        const seen = keyByPair.get(pair)

        if (seen !== undefined) {
          // Same pair, same key: the mapping is a function of its inputs.
          expect(leaseKey).toBe(seen)
          continue
        }

        keyByPair.set(pair, leaseKey)
      }

      const leaseKeys = [...keyByPair.values()]

      // Distinct pairs, distinct keys: no pair may alias another one's lock.
      expect(new Set(leaseKeys).size).toBe(leaseKeys.length)
    }
  )

  it('keeps the adversarial alphabet free of aliases', async () => {
    const seen = new Map<string, string>()

    for (const namespace of namespaces) {
      for (const key of logicalKeys) {
        const { leaseKey, channel } = await observe({ namespace, key })

        expect(leaseKey).toBe(leaseKeyFor(namespace, key))
        expect(channel).toBe(changeChannelFor(namespace, key))
        expect(seen.has(leaseKey)).toBe(false)

        seen.set(leaseKey, JSON.stringify([namespace, key]))
      }
    }

    expect(seen.size).toBe(namespaces.length * logicalKeys.length)
  })

  it('pins the layout: namespace, one sha256 slot tag, flight and change suffixes', async () => {
    const key = 'user:{42}:order'
    const byDefault = await observe({ key })
    const namespaced = await observe({ namespace: 'tenant-a', key })

    expect(byDefault.leaseKey).toBe(leaseKeyFor(DEFAULT_NAMESPACE, key))
    expect(byDefault.channel).toBe(changeChannelFor(DEFAULT_NAMESPACE, key))
    expect(namespaced.leaseKey).toBe(leaseKeyFor('tenant-a', key))

    // One tag, so every key derived from a logical key lands in the same slot
    // and a future multi-key script would stay valid on a cluster.
    const tags = namespaced.leaseKey.match(/\{[^}]*\}/g)
    expect(tags).toHaveLength(1)
    expect(tags![0]).toBe(`{${hashTag(key)}}`)
    expect(namespaced.leaseKey.endsWith('}:flight')).toBe(true)
    expect(namespaced.channel.endsWith('}:change')).toBe(true)
  })

  it('derives the key from the logical key alone, not from the lease', async () => {
    const first = await observe({ key: 'user:1', ttlMs: 1000 })
    const second = await observe({ key: 'user:1', ttlMs: 999_000 })

    expect(second.leaseKey).toBe(first.leaseKey)
    // The token is what makes a stale owner harmless, so every lease gets one.
    expect(second.token).not.toBe(first.token)
    expect(first.ttl).toBe('1000')
    expect(second.ttl).toBe('999000')
  })

  it('subscribes waiters to the channel the lease announces on', async () => {
    const observation = await observe({ key: 'user:1', subscribe: true })

    expect(observation.subscribedChannel).toBe(observation.channel)
  })

  it('substitutes a custom hashKey literally and validates nothing', async () => {
    // Slot safety of a custom `hashKey` is the caller's problem: the
    // coordinator substitutes whatever it returns. Pinned so that a change to
    // this layout is deliberate rather than accidental.
    const literal = await observe({
      key: 'user:1',
      hashKey: (key) => key.toUpperCase(),
    })
    const empty = await observe({ key: 'user:1', hashKey: () => '' })
    const brace = await observe({ key: 'user:1', hashKey: () => 'a:b}' })

    expect(literal.leaseKey).toBe('crossflight:{USER:1}:flight')
    // An empty tag still has its braces: the template wraps whatever it gets.
    expect(empty.leaseKey).toBe('crossflight:{}:flight')
    expect(brace.leaseKey).toBe('crossflight:{a:b}}:flight')
  })
})
