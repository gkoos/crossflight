import { composeDown, composeRun, composeUp } from './docker.js'

// Start Redis, run all tests (unit + integration) with coverage in one Docker container,
// then stop Redis. Produces a single combined coverage report with enforced thresholds.
let exitCode = composeUp(['redis'])
if (exitCode !== 0) process.exit(exitCode)

exitCode = composeRun('full-coverage')

const downCode = composeDown()

process.exit(exitCode === 0 ? downCode : exitCode)
