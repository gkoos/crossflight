import { spawnSync } from 'node:child_process'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

export const projectRoot = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '..'
)

/** Runs a command in the project root, inheriting stdio. */
const run = (command, args = []) => {
  const result = spawnSync(command, args, {
    cwd: projectRoot,
    stdio: 'inherit',
    shell: true,
  })
  if (result.error) throw result.error
  return result.status ?? 1
}

// Synchronous on purpose: these scripts are spawnSync-based, and the delay is
// only ever at the start of a job.
const sleepSync = (ms) => {
  if (ms > 0) {
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms)
  }
}

const RETRY_DELAYS_MS = [2_000, 10_000]

/**
 * `docker compose up` pulls its images, and a registry that resets a pull fails
 * the job on a flake that has nothing to do with the commit under test. Pulling
 * again is cheap - the layers that arrived are cached - so `up` is the one
 * command here that is retried; `compose run` is not, because repeating it would
 * repeat the tests, and a real test failure must never be retried away.
 */
const composeUp = (services) => {
  const args = ['compose', 'up', '-d', ...services]

  for (let attempt = 0; ; attempt += 1) {
    const status = run('docker', args)
    const delayMs = RETRY_DELAYS_MS[attempt]

    if (status === 0 || delayMs === undefined) {
      return status
    }

    process.stderr.write(
      `docker compose up failed with exit code ${status}; retrying in ${delayMs}ms\n`
    )
    sleepSync(delayMs)
  }
}

const composeRun = (service) =>
  run('docker', ['compose', 'run', '--rm', service])

const composeDown = () => run('docker', ['compose', 'down', '--remove-orphans'])

export { composeDown, composeRun, composeUp, run }
