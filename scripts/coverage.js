import { spawnSync } from 'node:child_process'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..')

const run = (command, args = []) => {
  const result = spawnSync(command, args, { cwd: projectRoot, stdio: 'inherit', shell: true })
  if (result.error) throw result.error
  return result.status ?? 1
}

// Start Redis, run all tests (unit + integration) with coverage in one Docker container,
// then stop Redis. Produces a single combined coverage report with enforced thresholds.
let exitCode = run('docker', ['compose', 'up', '-d', 'redis'])
if (exitCode !== 0) process.exit(exitCode)

exitCode = run('docker', ['compose', 'run', '--rm', 'full-coverage'])

const downCode = run('docker', ['compose', 'down', '--remove-orphans'])

process.exit(exitCode === 0 ? downCode : exitCode)
