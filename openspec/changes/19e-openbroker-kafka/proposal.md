## How to run this change
This change is fully designed: `design.md` holds the decisions, with the alternatives weighed, and `tasks.md` holds the units. Run it in a fresh session:

1. `git fetch`, then create a git worktree on a new branch off `origin/main`, and remove it after the merge.
2. Read this proposal, its specs, `design.md`, the `broker-sdk` and `broker-sdk-messaging` capabilities, and ADRs 0001-0017.
3. Re-check with ctx7 the latest `kafka-clients` 4.x, `testcontainers-kafka` and the newest `apache/kafka` image. If a library fact in `design.md` no longer holds, fix it with `/opsx:update` before applying. Ask only what is really the maintainer's to decide.
4. `/opsx:apply` with the harness under **Execution**. Each decision in `design.md` gets its ADR. A gap in the SDK is fixed in the SDK, for every driver, never worked around in this driver.
5. Verify: the driver's tests, the TCK, the messaging TCK and the chaos TCK against a real three-broker KRaft cluster on the oldest and newest supported Kafka; the Artemis driver still green; the JMH budgets met; japicmp reports the break as a minor bump.
6. PR, merge on green CI, `/opsx:archive`, and release `0.3.0`.

## Why
Kafka is the most widely run streaming platform, and its model is the furthest from the one driver OpenBroker has: a partitioned log, consumer groups with committed offsets, no per-message delete, retention instead of acknowledgement, and, since Kafka 4.2, share groups with per-record acknowledgement. A Kafka driver brings every OpenBroker consumer to Kafka estates, and it is the strongest test of the SDK. It runs while `0.x` still allows a breaking fix for every gap it finds.

## What Changes
- A new driver module, `openbroker-kafka`, built on the Apache Kafka clients (Admin, producer, consumer, share consumer), supporting Kafka 4.0 and later. It is in the BOM.
- Management: topics, partitions, consumer groups (classic, consumer and share), a group on a topic as a subscription whose backlog is its lag (an estimate, broken down by partition), ACLs, transactional ids, brokers as nodes and the controller quorum in health.
- Honest destructive operations: purge and seek of a subscription reset that group's offsets, which Kafka allows only on an empty group; truncate of a topic or partition deletes records for every group. All are previewed with their reach. Per-message move, copy and delete are declared unavailable, with the reason.
- Browse by partition, offset and time, which never joins a group or commits.
- The messaging client on Kafka:
  - at most once, at least once and transacted guarantees, with joined transactions (consume, transform, produce);
  - offsets as settlement for consumer groups, with ordering per partition and per key;
  - cooperative rebalances under the stated loss and duplicate contract;
  - client-enforced retry and dead-letter topics;
  - share groups as a second, unordered subscription kind with broker-enforced delivery counts.
- **BREAKING** SDK fixes that Kafka exposes, each for every driver:
  - the deduplication scope a transport declares;
  - credentials that can be a bearer token;
  - a default retry mode that follows what the transport enforces;
  - a display label on every kind;
  - a chaos fixture that names the serving node and its successor instead of an HA pair.
- The CI matrix gains Kafka: `verify` and `chaos` run against the oldest and newest supported `apache/kafka`.

## Capabilities
### New Capabilities
- `broker-sdk-kafka`: the Apache Kafka driver, its resource model, its destructive operations, its messaging client and its conformance.

### Modified Capabilities
- `broker-sdk`: kinds carry a display label; credentials take a bearer-token form; the chaos fixture is generalised.
- `broker-sdk-messaging`: idempotence declares its scope; the default retry mode follows the transport's enforcement; the chaos suite covers a replicated log.

## Out of scope
- Kafka Streams, Kafka Connect and schema registries.
- Partition reassignment and broker configuration changes.
- Managing the system's deployment (operators, Helm, cloud services) and cloud-specific authentication (for example IAM-signed SASL).
- ZooKeeper-based clusters (Kafka before 4.0).

## Depends on
- 19d2-openbroker-messaging (the messaging client and its TCK, released in `0.2.0`)

## Execution
**Subagent-driven + planner**: the planner reviews the SDK gaps before any driver code. Then the management surface, the consumer-group messaging, the share-group messaging and the chaos fixture are separable units, each with its TCK cases.

Drivers touched: `openbroker-kafka` (new) and `openbroker-artemis` (labels, the chaos fixture, the default retry mode). It breaks the API, so the next release is `0.3.0`.

## Impact
New module `openbroker-kafka`; `openbroker-api` (`Kind`, `Credentials`, `TransportCapabilities`, `RetryPolicy` defaults); `openbroker-tck` (`TckChaos`, new Kafka-shaped cases); `openbroker-artemis` (labels, fixture); `openbroker-benchmarks` (Kafka paths); the BOM; `ci.yml`; README and ADRs 0018 and later. New dependencies are `org.apache.kafka:kafka-clients` and, for tests, `org.testcontainers:testcontainers-kafka`.
