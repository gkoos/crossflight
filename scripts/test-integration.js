import { composeDown, composeRun, composeUp } from './docker.js'

// Start Redis, run the integration suite in a container, then stop Redis.
let exitCode = composeUp(['redis'])
if (exitCode === 0) {
  exitCode = composeRun('redis-integration-test')
}
const downCode = composeDown()
process.exit(exitCode === 0 ? downCode : exitCode)
