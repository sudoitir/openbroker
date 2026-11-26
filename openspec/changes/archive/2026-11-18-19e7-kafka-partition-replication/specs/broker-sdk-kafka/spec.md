## ADDED Requirements

### Requirement: Every replica reports its size and how far it lags

The driver SHALL report, for each replica of a partition, the broker, whether it is in the in-sync set, the bytes that replica holds, and how far its log lags the leader, in offsets. The bytes SHALL be read once for the brokers that hold the page, not once per partition. A broker that does not answer SHALL be named, and the replicas on brokers that did answer SHALL still be reported. The leader's size SHALL remain available when a follower's broker does not answer. Bytes SHALL NOT be presented as a record count.

#### Scenario: A follower is behind

- **WHEN** a caller reads a partition whose follower is 40 offsets behind the leader
- **THEN** the follower is listed with its byte size and an offset lag of 40, and the leader is listed with its own size

#### Scenario: One broker does not answer

- **WHEN** one of three brokers does not answer the log-directory read
- **THEN** that broker is named, and the replicas on the other two brokers are still reported

### Requirement: Local and tiered offsets are a capability

Where the cluster can answer a local log start and a tiered offset, the driver SHALL report both on the partition. Where the cluster rejects that read, the driver SHALL declare the capability unavailable with the broker's reason and SHALL NOT invent the numbers. Where the topic is not tiered, the local log start SHALL equal the log start and the tiered offset SHALL be reported as unknown, with the reason that the topic is not tiered. Eligible leader replicas SHALL keep the capability the driver already declares.

#### Scenario: A tiered topic

- **WHEN** a caller reads a partition of a topic that stores data remotely, on a cluster that supports the read
- **THEN** the partition shows its local log start and its tiered offset

#### Scenario: A cluster that cannot report them

- **WHEN** a caller reads the capability on a cluster that rejects the local and tiered offset read
- **THEN** the capability is unavailable with the broker's reason, and no local or tiered offset is invented

#### Scenario: A topic that is not tiered

- **WHEN** a caller reads a partition of a topic that is not tiered, on a cluster that supports the read
- **THEN** the local log start equals the log start and the tiered offset is unknown, with the reason that the topic is not tiered

### Requirement: Preferred-leader election is previewed and preferred only

The driver SHALL preview an election of the preferred leader for a set of partitions. The preferred leader is the first replica. The preview SHALL name each partition whose leader would change and each that already has its preferred leader, and SHALL carry an etag over the current leaders and replica sets. Apply SHALL ask the broker for a preferred election only. An unclean election SHALL NOT be offered. An apply whose etag no longer matches SHALL be refused and change nothing. Where the broker rejects the operation as unsupported, the driver SHALL report that capability unavailable with the broker's reason.

#### Scenario: One partition is not on its preferred leader

- **WHEN** a caller previews a preferred election for two partitions, and only the first has a leader other than its first replica
- **THEN** the preview names the first as changing and the second as already preferred, and applying it elects the preferred leader of the first

#### Scenario: Leaders moved after the preview

- **WHEN** a partition's leader changes between the preview and the apply
- **THEN** the apply is refused as stale and no election is sent

### Requirement: Reassignment is previewed, throttled and cancellable

The driver SHALL preview a new replica set for a partition, showing the current replicas, the new replicas, the replication factor before and after, and an estimate of the bytes to copy (the sizes of the replicas that would be added). The preview SHALL carry the throttle the caller set, in bytes per second, and an estimate of duration as those bytes divided by that rate, labeled an estimate. A new replication factor below the topic's `min.insync.replicas` SHALL be refused in the preview, naming both numbers, and SHALL NOT be applied. The preview SHALL carry an etag over the current replicas, the in-sync set, the leader and any reassignment already in progress for that partition. An apply whose etag no longer matches, including one that overlaps a reassignment already in progress, SHALL be refused and change nothing.

The driver SHALL list reassignments in progress. Cancelling one SHALL be its own preview and SHALL use the same etag rule.

When a reassignment this driver started finishes, the throttle entries this operation added SHALL be removed, including after the caller restarts. A throttle value that was already set SHALL be restored to the value it had, and the preview SHALL have said so. The driver SHALL NOT clear a throttle it did not set.

Where the broker rejects reassignment as unsupported, the driver SHALL report that capability unavailable with the broker's reason.

#### Scenario: Adding a replica

- **WHEN** a caller previews adding broker 3 to a partition that is on brokers 1 and 2, with a throttle, and then applies it
- **THEN** the preview shows the current replicas, the new replicas, the bytes to copy as an estimate, and the throttle, and the partition's reassignment is in progress

#### Scenario: Replication factor below min.insync.replicas

- **WHEN** a caller previews a replica set of 2 for a topic whose `min.insync.replicas` is 3
- **THEN** the preview is refused, names both numbers, and the replica set is unchanged

#### Scenario: A throttle that was already there

- **WHEN** a caller starts a reassignment while a replication throttle is already set, and the reassignment then finishes
- **THEN** the throttle is back to the value it had before this operation

#### Scenario: Cancelling

- **WHEN** a caller previews cancellation of a reassignment that is in progress and applies it
- **THEN** the reassignment is no longer in progress and the partition's replicas are unchanged from before the move

#### Scenario: Another move is already running

- **WHEN** a caller applies a reassignment for a partition that already has one in progress
- **THEN** the apply is refused as stale and the in-progress reassignment is left as it was
