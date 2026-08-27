import { spawnSync } from 'node:child_process'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..')

const run = (command, args = []) => {
  const result = spawnSync(command, args, { cwd: projectRoot, stdio: 'inherit', shell: true })
  if (result.error) throw result.error
  if (typeof result.status === 'number' && result.status !== 0) process.exit(result.status)
}

const defaultArgs = [
  '--iterations=1',
  '--loader-ms=2000',
  '--key=bench:coalesce',
  '--instances=20',
  '--requests-per-instance=20',
  '--concurrency=8',
]

const benchmarkArgs = [...defaultArgs, ...process.argv.slice(2)]

console.log('Benchmark args:', benchmarkArgs.join(' '))

run('docker', ['compose', 'up', '-d', 'redis'])

try {
  run('npx', ['tsx', 'benchmarks/benchmark-cli.ts', ...benchmarkArgs])
} finally {
  run('docker', ['compose', 'down', '--remove-orphans'])
}
