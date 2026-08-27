import { parentPort } from 'node:worker_threads'
import { pbkdf2Sync } from 'node:crypto'

const BENCHMARK_SALT = 'crossflight-benchmark-salt'

parentPort.on('message', ({ targetMs }) => {
  const startedAt = Date.now()
  let value = 'crossflight'

  while (Date.now() - startedAt < targetMs) {
    value = pbkdf2Sync(value, BENCHMARK_SALT, 80_000, 32, 'sha256').toString('hex')
  }

  parentPort.postMessage({ result: value })
})
