# Design: Kafka 3.0 and later, empty classic groups, and reach in verify

## Context

The Kafka driver supports `[4.0.0,)`: `ClusterVersions` refuses a cluster whose software supports no
`metadata.version` level 22. Many running estates are on Kafka 3.x, often in ZooKeeper mode, which the
4.x clients speak fully. Classic application groups, still the default protocol of most clients, are
read-only (ADR-0018), so an empty one cannot be rewound even though Kafka allows it. `verify` checks the
cluster through whichever broker answers, so a broker that advertises an address the caller cannot
reach passes the check and fails later.

## Goals / Non-Goals

**Goals:** Kafka 3.0+ in ZooKeeper and KRaft mode; what a cluster lacks declared per feature with its
reason; empty, idle classic groups as subscriptions; `verify` naming every broker's reach. No change to
`openbroker-api`.

**Non-Goals:** Kafka before 3.0; resetting streams groups; emulating any feature a cluster lacks.

## Decisions

### D1. One feature read per connection (`admin/ClusterFeatures`)
A new public record `ClusterFeatures(kraft, release, OptionalInt metadataLevel, finalized levels of
group.version, streams.version, eligible.leader.replicas.version, transaction.version, ShareFeatures
share)` with `read(Admin, Await)`, `describe()`, `require(String supportedVersions)`,
`consumerProtocol()`, `streamsGroups()`, `fencedBrokers()`, `eligibleLeaders()`, `assessments()`.
`KafkaCluster` caches it (`volatile`), `features()` reads it lazily, `readFeatures()` re-reads, and
`reconfigure` clears it. `ClusterVersions` and `ClusterVersionsTest` are deleted;
`ShareFeatures.of(FeatureMetadata)` replaces the copy in `ClusterVersions.sharesInProduction`, and
`Handlers` uses `cluster.features().share().available()`.

**Reading it:**
1. `describeFeatures()`: the supported maximum of `metadata.version` (`L`, possibly absent) and the
   finalized levels above.
2. Mode: `describeMetadataQuorum()`. `UnsupportedVersionException` → ZooKeeper; success or an
   authorization failure → KRaft; anything else is rethrown.
3. Release: with `L`, `floorEntry(L)` in a table of each release's first level, copied from
   `MetadataVersion.java` at tag 4.4.0 (never from memory). The newest entry renders "at least X".
   Without `L`, probe `listTransactions()` (KIP-664): unsupported → refused; success → "at least 3.0".
   `KafkaBefore30ZooKeeperIT` proves the marker; if a 2.8 broker answers it, use DescribeTransactions.
4. Refusal: `FAILED_PRECONDITION`/`UNSUPPORTED_VERSION`, "This cluster runs a Kafka before 3.0 (…);
   the Kafka driver supports [3.0.0,)."
5. `describe()`: `"<release>, KRaft mode"` or `"at least 3.0, ZooKeeper mode"`.

- Rejected: ApiVersions maxima. The public `Admin` does not expose them; only internal client classes
  do, and those break on upgrades.

### D2. What a cluster lacks is declared, never emulated
`KafkaAccess.probe()` adds `features.assessments()` as capabilities, `UNAVAILABLE` with a reason (and
a remedy where an operator can change it):

| Capability | Available when | Reason when not |
|---|---|---|
| `kafka.quorum` | KRaft | "This cluster runs in ZooKeeper mode, which has no KRaft controller quorum to describe." |
| `kafka.consumer-protocol-groups` | `group.version` ≥ 1 | "Consumer-protocol groups (KIP-848) need Kafka 4.0 or later with group.version finalized at 1 or more; this cluster finalizes it at N." Remedy on 4.x: `kafka-features.sh upgrade --feature group.version=1` |
| `kafka.share-groups` | existing `ShareFeatures` rule | existing reason and remedy |
| `kafka.streams-groups` | `streams.version` ≥ 1 | "Streams groups need streams.version finalized at 1 or more; this cluster finalizes it at N." |
| `kafka.fenced-brokers` | KRaft and `L` ≥ 22 | "Kafka before 4.0 does not list fenced brokers, so a fenced broker is missing from the nodes instead of shown as fenced." |
| `kafka.eligible-leaders` | `eligible.leader.replicas.version` ≥ 1 | "Eligible leader replicas (KIP-966) are not enabled on this cluster." |

KIP-890 (`transaction.version`) is information only. `listGroups()` without types works on old
brokers; an empty `GroupListing.type()` is CLASSIC (`GroupInfo.typeOf`).

`KafkaNodes`: in ZooKeeper mode it skips `describeMetadataQuorum` and adds no OUT_OF_SYNC condition;
`includeFencedBrokers(true)` only when `fencedBrokers()`; node attributes
`kafka.cluster_version` (the release text) and `kafka.cluster_mode` (`kraft`|`zookeeper`).
`NodeDescription.version` stays empty. `Partitions`/`KafkaKinds`: `kafka.elr` and
`kafka.last_known_elr` only when `eligibleLeaders()`. `KafkaDriver` range `[3.0.0,)`.
`messaging/Consumption`: `group.protocol=consumer` when `consumerProtocol()`, otherwise `classic` with
`CooperativeStickyAssignor`: Kafka's own protocol, not an emulation.

### D3. Empty, idle classic groups are subscriptions (ADR-0031, superseding ADR-0018's classic-group point)
`GroupInfo.manageable()` is CONSUMER, SHARE or CLASSIC; STREAMS stays read-only, and a classic group
whose assignor is Kafka Streams' (`stream`) stays read-only too, with the reason that the streams
application owns its offsets and state. `Subscriptions.accepts` takes any classic group.
`requireEmpty` becomes `requireIdle`: members are refused as today, and a rebalancing state
(PREPARING_REBALANCE, COMPLETING_REBALANCE, ASSIGNING, RECONCILING) is refused with
`FAILED_PRECONDITION`/`NOT_EMPTY`: "Cannot <what> <id> while <group> is rebalancing (<state>). Try
again once it settles." A preview of an empty classic group whose assignor cannot be read notes that a
Kafka Streams application's state must be reset with Kafka's application reset tool. `Groups` comments
and texts follow.

- Rejected: keeping classic groups read-only. Kafka allows the reset only on an empty group, so no
  running application's offsets move, and the default client protocol would otherwise be unmanageable.

### D4. `verify` reaches every broker (`admin/Reach`)
`KafkaConnection.verify`: `features = cluster.readFeatures(); features.require(range)`, then
`nodes.describe(whole)`, then `Reach.of(cluster, described, deadline)`, then `access.probe()`.
`Reach` runs one virtual thread per BROKER node, budget `min(connect timeout, deadline remaining)`,
bounded overall by `budget + 250 ms`:
1. `InetAddress.getAllByName(host)`: `UnknownHostException` → `dns`.
2. TCP connect to each address, closed at once: all refused → `refused`; timeout → `timeout`.
3. `describeConfigs(BROKER <id>)`, which the Admin sends to that broker: success or PERMISSION_DENIED
   → reached; TLS_HANDSHAKE_FAILED → `tls` (UNAVAILABLE); CREDENTIALS_REJECTED → `authentication`
   (UNAUTHENTICATED); no answer → `timeout` ("connected to h:p, but the broker did not answer within Ns").

Each failure is a `BrokerException` with `Dispatch.NOT_SENT`, the node, and `ErrorInfo` metadata
`kafka.endpoint` and `kafka.reach_failure`; its message names both ("Broker 2 advertises kafka-2:9092,
which does not resolve."). The `Partial` holds FAILED for an unreached broker, SKIPPED for
controller-only nodes, and keeps every description.

### D5. TCK (additive)
`OpenBrokerTck.verifyNamesEveryNodesOutcome`: on a healthy cluster every node has an outcome, each
APPLIED or SKIPPED, and no failures. `ChaosTck.verifyReportsACutNodeAndTheRest` (skipped when
`haRequest()` names fewer than two nodes): cut the last node, verify returns, the cut node FAILED with
UNAVAILABLE or DEADLINE_EXCEEDED, the rest APPLIED or SKIPPED; heal in `finally`.

### D6. Fixtures and CI matrix
Testcontainers 2.x supports cp-kafka only from 7.4 in KRaft, so a `ZooKeeperKafkaBroker extends
GenericContainer` (cp-kafka, advertised listeners written by a start script, wait on
`[KafkaServer id=\d+] started`) plus a cp-zookeeper container. `KafkaTestCluster.start(image)` picks it
for `confluentinc/cp-kafka`; `brokers()` becomes `List<GenericContainer<?>>`; `supports(feature, level)`
gates tests instead of image prefixes. Failsafe excludes `*ZooKeeperIT` by default; profile `zookeeper`
sets `kafka.image=confluentinc/cp-kafka:7.0.16` and runs the TCKs and ZooKeeper ITs. CI's Kafka matrix:
`cp-kafka:7.0.16` (`-Pzookeeper`), `apache/kafka:3.9.2`, `4.0.2`, `4.4.0`; chaos on the newest only.

## Risks / Trade-offs
- [The 3.0 marker is unproven] → `KafkaBefore30ZooKeeperIT` (cp-kafka 6.2.15, Kafka 2.8) proves it.
- [ZooKeeper clusters below 3.3 with a broker down] → `listTransactions` fans out and fails; verify
  then fails as unreachable rather than per node. Stated in the README.
- [Early KRaft 3.0–3.2 is labelled ZooKeeper mode] → it was never production-ready.
- [The raw TCP check logs a closed connection on TLS listeners] → accepted.
- [Memory] → the ZooKeeper leg is about 2.5 GB; run legs one at a time.
