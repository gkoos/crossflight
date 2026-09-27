import { runCoordinatorContract } from './coordinator-contract.js'
import { EventedMemoryCoordinator } from './mocks/evented-memory-coordinator.js'
import { InMemoryCoordinator } from './mocks/in-memory-coordinator.js'

runCoordinatorContract(
  'evented memory coordinator',
  () => new EventedMemoryCoordinator(),
  { notifications: true, closesOperations: true }
)

// The in-memory double is deliberately minimal: it owns leases but has no
// wake-up signal and no closed state, so only the core of the contract applies.
runCoordinatorContract(
  'in-memory coordinator double',
  () => new InMemoryCoordinator()
)
