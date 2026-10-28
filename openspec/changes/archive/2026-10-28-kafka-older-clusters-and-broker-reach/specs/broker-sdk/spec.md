## ADDED Requirements

### Requirement: Verify names every node's outcome
Verify SHALL return a result that states, for every node of the cluster, whether it was reached, failed or skipped, with the failure for each node that failed. A node that is unreachable SHALL NOT stop the description of the others.

#### Scenario: A healthy cluster
- **WHEN** a consumer verifies a connection to a healthy cluster
- **THEN** every node has an outcome, each applied or skipped, and none failed

#### Scenario: One node cut
- **WHEN** a consumer verifies a connection while one node of the cluster is cut off
- **THEN** that node's outcome is failed or in doubt, with a failure of `UNAVAILABLE` or `DEADLINE_EXCEEDED`, and every other node is applied or skipped
