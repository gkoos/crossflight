import { spawnSync } from 'node:child_process'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..')

const run = (command, args = []) => {
  const result = spawnSync(command, args, { cwd: projectRoot, stdio: 'inherit', shell: true })
  if (result.error) throw result.error
  return result.status ?? 1
}

let exitCode = run('docker', ['compose', 'up', '-d', 'redis'])
if (exitCode === 0) {
  exitCode = run('docker', ['compose', 'run', '--rm', 'redis-integration-test'])
}
const downCode = run('docker', ['compose', 'down', '--remove-orphans'])
process.exit(exitCode === 0 ? downCode : exitCode)
