import { Redis as IORedis } from 'ioredis'
import { describe, expect, it } from 'vitest'

const shouldRun = process.env.RUN_REDIS_CLUSTER_INTEGRATION === '1'

describe.runIf(shouldRun)('redis cluster integration', () => {
  it('connects to a running Redis Cluster and exposes all expected nodes', async () => {
    const client = new IORedis.Cluster([
      { host: '172.28.0.11', port: 7001 },
      { host: '172.28.0.12', port: 7002 },
      { host: '172.28.0.13', port: 7003 },
      { host: '172.28.0.14', port: 7004 },
      { host: '172.28.0.15', port: 7005 },
      { host: '172.28.0.16', port: 7006 },
    ])

    try {
      const clusterNodes = await client.cluster('NODES')
      const lines = clusterNodes
        .split('\n')
        .map(line => line.trim())
        .filter(Boolean)

      expect(lines.length).toBeGreaterThanOrEqual(6)
      expect(lines.some(line => line.includes(':7001'))).toBe(true)
      expect(lines.some(line => line.includes(':7006'))).toBe(true)
      expect(await client.ping()).toBe('PONG')
    } finally {
      await client.quit()
    }
  })
})
