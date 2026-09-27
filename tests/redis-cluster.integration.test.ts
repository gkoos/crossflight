import { createHash } from 'node:crypto'

import { Cluster as IORedisCluster } from 'ioredis'
import { afterEach, describe, expect, it } from 'vitest'

import { redisCoordinator } from '../src/coordinators/redis.js'
import type { Coordinator } from '../src/types.js'
import { runCoordinatorContract } from './coordinator-contract.js'

const shouldRun = process.env.RUN_REDIS_CLUSTER_INTEGRATION === '1'

type ClusterNode = { host: string; port: number }

// The compose stack pins the nodes to these addresses on a private network, so
// this suite runs inside that network (docker compose run redis-cluster-test).
const DEFAULT_NODES: ClusterNode[] = [
  { host: '172.28.0.11', port: 7001 },
  { host: '172.28.0.12', port: 7002 },
  { host: '172.28.0.13', port: 7003 },
  { host: '172.28.0.14', port: 7004 },
  { host: '172.28.0.15', port: 7005 },
  { host: '172.28.0.16', port: 7006 },
]

function startupNodes(): ClusterNode[] {
  const configured = process.env.REDIS_CLUSTER_NODES
  if (!configured) {
    return DEFAULT_NODES
  }

  return configured.split(',').map(entry => {
    const [host, port] = entry.trim().split(':')
    return { host, port: Number(port) }
  })
}

const open: Array<{ coordinator: Coordinator; client: IORedisCluster }> = []

function openCluster(): { client: IORedisCluster; coordinator: Coordinator } {
  const client = new IORedisCluster(startupNodes())
  const coordinator = redisCoordinator(client)
  open.push({ coordinator, client })
  return { client, coordinator }
}

// The contract suite is the executable coordinator specification: a cluster
// client is supported when it passes it unchanged.
runCoordinatorContract(
  'redis cluster coordinator',
  () => openCluster().coordinator,
  { run: shouldRun, notifications: true, closesOperations: true }
)

// Clean up even when an assertion failed mid-test.
afterEach(async () => {
  for (const { coordinator, client } of open.splice(0)) {
    await coordinator.close().catch(() => undefined)
    await client.quit().catch(() => undefined)
  }
})

describe.runIf(shouldRun)('redis cluster integration', () => {
  it('connects to a running Redis Cluster and exposes all expected nodes', async () => {
    const { client } = openCluster()

    const clusterNodesRaw = await client.cluster('NODES')
    if (typeof clusterNodesRaw !== 'string') {
      throw new Error('Expected Redis CLUSTER NODES response to be a string')
    }

    const lines = clusterNodesRaw
      .split('\n')
      .map(line => line.trim())
      .filter(Boolean)

    expect(lines.length).toBeGreaterThanOrEqual(6)
    expect(lines.some(line => line.includes(':7001'))).toBe(true)
    expect(lines.some(line => line.includes(':7006'))).toBe(true)
    expect(await client.ping()).toBe('PONG')
  })

  it('pins a lease key and its change channel to the same cluster slot', async () => {
    const { client, coordinator } = openCluster()
    const key = 'cluster:slot:key'
    const lease = await coordinator.acquire(key, { ttlMs: 1000 })
    expect(lease).not.toBeNull()

    const tag = createHash('sha256').update(key).digest('hex')
    expect(lease!.key).toBe(`crossflight:{${tag}}:flight`)

    const leaseSlot = await client.cluster('KEYSLOT', lease!.key)
    const channelSlot = await client.cluster(
      'KEYSLOT',
      `crossflight:{${tag}}:change`
    )

    // Both artifacts are routed by the tag, so a waiter subscribes on the
    // node that holds the lease it is waiting on.
    expect(channelSlot).toBe(leaseSlot)
    expect(await client.cluster('KEYSLOT', `{${tag}}`)).toBe(leaseSlot)

    await lease!.complete()
  })

  it('keeps ownership exclusive and wakes waiters across cluster nodes', async () => {
    const owner = openCluster()
    // A client that starts discovery from another node still routes the key to
    // the same shard: both coordinators talk about the same lease.
    const other = new IORedisCluster([...startupNodes()].reverse())
    const waiter = redisCoordinator(other)
    open.push({ coordinator: waiter, client: other })

    const key = 'cluster:cross-node:key'
    const lease = await owner.coordinator.acquire(key, { ttlMs: 2000 })
    expect(lease).not.toBeNull()

    await expect(waiter.acquire(key, { ttlMs: 2000 })).resolves.toBeNull()

    const waiting = waiter.waitForChange(key, { timeoutMs: 2000 })
    // Let the subscription settle before the change is signalled.
    await new Promise(resolve => setTimeout(resolve, 100))

    const startedAt = Date.now()
    await lease!.complete()

    await expect(waiting).resolves.toBeUndefined()
    expect(Date.now() - startedAt).toBeLessThan(1000)

    const reacquired = await waiter.acquire(key, { ttlMs: 1000 })
    expect(reacquired).not.toBeNull()
    await reacquired!.complete()
  })
})
