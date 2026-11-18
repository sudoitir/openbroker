# ADR-0037: A Kafka replica move is previewed, and only a preferred election is sent

- **Status**: accepted
- **Date**: 2026-11-18
- **Deciders**: Mahdi Amirabdollahi

## Context

`Operation.Reassign` and `Operation.ElectLeader` are already in the SDK. The Kafka driver implemented neither, so a caller could not move a partition's replicas or hand leadership back to the first replica. Both calls have been in the Admin API since Kafka 2.4, which the 2.8 floor speaks, in ZooKeeper mode and in KRaft.

`describeLogDirs` was already used, and only for the leaders of a page, through one future for the whole set. One broker that failed dropped every size on the page. `ReplicaInfo.size()` is the bytes on that broker and does not include remote storage. `offsetLag()` is the lag that broker reports. `electLeaders` with a null partition set elects every partition. `OffsetSpec.earliestLocal` and `OffsetSpec.latestTiered` need Kafka 3.9. A change of replication factor is rejected unless `allowReplicationFactorChange` is set.

ADR-0005 already says an operation is previewed against an etag, and that volume is not part of that etag. ADR-0031 already says a feature the cluster lacks is declared unavailable and is not emulated.

## Decision

- The partition kind declares `REASSIGN` and `ELECT_LEADER`.
- One `describeLogDirs` covers every broker that holds a replica on the page. Each broker is its own future. A broker that fails is named on the page, and the replicas on brokers that answered are still returned. A future replica is named as future. The leader-only path is removed.
- Local log start and the tiered offset are the capability `kafka.tiered-offsets`. The driver tries the read once per connection. A refusal marks the capability unavailable with the broker's reason and is not retried. A topic that is not tiered reports local start equal to log start and a tiered offset of unknown, reason "not tiered". No number is invented.
- Preferred election sends `ElectionType.PREFERRED` only. `Election.UNCLEAN` stays in the enum. The Kafka driver refuses it with `FAILED_PRECONDITION` and sends nothing. An empty set is the same refusal. `electLeaders` is never called with null. The etag is the leader and the replica order. An in-progress reassignment refuses the election.
- `Operation.Reassign` gains an `Attributes options` component. A one-argument constructor delegates to empty options, so existing constructions keep compiling. The Kafka driver reads `replication.throttle.bytes_per_second`. A missing or non-positive rate is refused before any alter. Other drivers ignore options they do not understand.
- A reassignment preview names the current replicas, the new replicas, both replication factors, the bytes to copy as an estimate (the sizes of replicas that would be added), the throttle, and a duration estimate of those bytes divided by the rate. A missing size makes both estimates unavailable. A replication factor below the topic's `min.insync.replicas` is refused in the preview, naming both numbers, with no alter. The alter uses `allowReplicationFactorChange(true)`.
- The etag is the replica order, the in-sync set, the leader, and the id of a reassignment already in progress. Bytes and lag are not in it. A mismatch, including a second move of the same partition, is `FAILED_PRECONDITION` and sends nothing. Cancel is a preview of the in-progress reassignment, applied as an empty assignment.
- Before the alter, the driver reads the current replication throttle. The result resource carries those previous values and which keys this call added. The driver does not store them. The caller restores them when the reassignment is no longer listed. If the alter fails after this call changed a throttle, the same call writes the previous values back before it returns the error. A key this call did not change is not written back.

## Consequences

- A caller can show placement per replica, and can preview a move or a preferred election before anything changes.
- The canonical constructor of `Reassign` gains a component. The 0.12.0 note says so. Source that uses the one-argument constructor does not change.
- A caller that wants the throttle removed when the move finishes has to keep the baseline the result returned. The driver will not remember it across a restart.
- An unthrottled move is not available. A caller that wants one has no path.
- Unclean election remains unrepresentable as a successful Kafka call. A system that has it uses the same enum and its own driver.

## Alternatives considered

- **Leave size as the leader's bytes only.** Rejected. The question during an incident is which replica is on which disk.
- **Gate election and reassignment on a version number.** Rejected. Both calls exist on the 2.8 floor. A broker that rejects them is an unavailable capability, which is the rule ADR-0031 already has.
- **Persist the throttle baseline in the driver.** Rejected. The driver has no database. Remembering across a restart is the caller's.
- **Send an unclean election when the caller asks for `Election.UNCLEAN`.** Rejected. It can drop acknowledged records. The enum stays so another system can implement it. This driver does not.
