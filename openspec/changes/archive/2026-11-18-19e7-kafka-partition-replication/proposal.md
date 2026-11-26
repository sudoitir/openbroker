# Kafka partition replication

Archived 2026-11-27. Merged and released; the requirements it added live in `broker-sdk-kafka`.

## Why

An operator needs the partition, not the broker count: which leader, which replicas are in sync, which
replica is offline, whether the preferred leader is leading, and how many bytes sit on each disk. The
driver already read most of that, and a caller had no way to hand leadership back to the first replica
or to move a partition's replicas, which the Admin API has supported since 2.4 in ZooKeeper and KRaft
mode. Both belong in the SDK, previewed and gated by the caller.

## What Changes

- A partition reports, per replica, the broker, the in-sync flag, the bytes on that broker and the
  offset lag, from one log-directory read for the brokers of a page. A broker that does not answer is
  named and the others still report.
- Local log start and the tiered offset are a capability, declared unavailable with the broker's
  reason where the cluster cannot answer, and reported as "not tiered" where the topic is not.
- A preferred-leader election is previewed and sends `ElectionType.PREFERRED` only. An unclean
  election is not an operation the driver performs.
- A replica reassignment is previewed with the bytes to copy as an estimate, the throttle and a
  duration estimate, refuses a replication factor below `min.insync.replicas`, and can be cancelled
  through its own preview. The result carries the throttle values from before the call and which keys
  this call added, so a caller can put them back.

## Capabilities

### Modified Capabilities

- `broker-sdk-kafka`: Per-replica log directory fields, tiered offsets as a capability, and
  previewed reassignment and preferred-leader election.

## Impact

- `openbroker-kafka`: partition reads and two operations. No new dependency, no API type removed.
- `Operation.Reassign` gained an options component; the one-argument constructor still compiles.

## Out of scope

Unclean leader election, a balancer that mutates on its own, and changing a topic's partition count.

## Execution

Done. The driver change merged on green CI and the release that contains it is published to Maven
Central. ADR-0037 records the decision.
