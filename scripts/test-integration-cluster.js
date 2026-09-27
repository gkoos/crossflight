import { composeDown, composeRun, composeUp } from './docker.js'

// The cluster comes up as six nodes plus the initialisation container: `up`
// pulls their images, and the initialisation has to finish before the tests run.
const CLUSTER_SERVICES = [
  'redis-node-1',
  'redis-node-2',
  'redis-node-3',
  'redis-node-4',
  'redis-node-5',
  'redis-node-6',
  'redis-cluster-init',
]

let exitCode = composeUp(CLUSTER_SERVICES)
if (exitCode === 0) {
  exitCode = composeRun('redis-cluster-test')
}
const downCode = composeDown()
process.exit(exitCode === 0 ? downCode : exitCode)
