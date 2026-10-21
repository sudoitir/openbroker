# Design: the Kafka driver

## Context

OpenBroker `0.2.0` has one driver, Artemis, and a production messaging client (ADRs 0010-0017):
- declared guarantees (`Guarantee`, `TransportCapabilities`);
- driver-owned recovery (`Recovery`, `LinkState`);
- bounded sending (`SendBuffer`, `Offer`);
- processing lanes on virtual threads;
- retry policies (`RetryMode.SYSTEM` by default, `MANAGED`);
- an observer hook;
- JMH budgets;
- `MessagingTck` and `ChaosTck`.

`ChaosTck` drives a `TckChaos` fixture that has a shared-store `primary()` and `backup()`. Chaos uses Toxiproxy and Docker kill and start.

Kinds have ids (`<system>.<name>`) but no label. `Credentials` is a username and password only. `ClusterSpec` carries nodes, cluster endpoints and a string options map that the driver describes through `OptionSpec`.

The resource-class mapping fixed when the SDK was designed gives:
- Kafka topic → DESTINATION
- partition → PARTITION
- consumer group → CONSUMER_GROUP
- group on a topic → SUBSCRIPTION
- ACL → ACCESS_RULE
- transactional id → TRANSACTION

Under that mapping, browse is by offset or time, truncate is `deleteRecords`, notifications come from polling, and per-message move and delete are unimplemented.

Library facts were checked with ctx7 against the Kafka clients 4.3 javadoc and the Kafka docs:
- `Admin.alterConsumerGroupOffsets` requires an empty group.
- `deleteRecords` raises `PolicyViolationException` on a compact-only topic.
- `OffsetSpec` has `earliest`, `latest` and `forTimestamp`.
- `KafkaShareConsumer` acknowledges with `AcknowledgeType.ACCEPT`, `RELEASE` and `REJECT` in explicit mode.
- Share groups are production-ready in Kafka 4.2, and share-group dead-lettering needs `share.version` 2.
- `sendOffsetsToTransaction` takes the consumer's `ConsumerGroupMetadata`.
- `ConsumerRebalanceListener.onPartitionsLost` separates lost partitions from revoked ones.
- Testcontainers' `org.testcontainers.kafka.KafkaContainer` runs the `apache/kafka` image.

## Goals / Non-Goals

**Goals:**
- A driver that is honest about Kafka: it declares what Kafka cannot do and never emulates it.
- The full messaging client on both of Kafka's consumption models.
- SDK gaps fixed once, for every driver, while `0.x` allows the break.

**Non-Goals:**
- Kafka Streams, Connect, schema registries, partition reassignment, broker configuration and quotas.
- Per-message delete by copy-and-skip, which would lie about the log.
- Clusters before 4.0.

## Decisions

### D1. Build on `kafka-clients` alone
The driver uses `org.apache.kafka:kafka-clients` (Admin, `KafkaProducer`, `KafkaConsumer`, `KafkaShareConsumer`) and nothing else at runtime.
- **Spring for Apache Kafka was rejected.** It would put Spring into a framework-free driver (non-negotiable 1). Its listener containers, `DefaultErrorHandler` and retry topics would also duplicate the SDK's lanes and policies, giving two engines with two behaviours.
- **Confluent Parallel Consumer was rejected for per-key concurrency.** It owns its threads, its commit cadence and its retry. Those clash with lanes on virtual threads (ADR 0013) and with retry policies (ADR 0014), and it adds a dependency whose maintenance pace is slower than the clients'.

Its two core ideas, contiguous offset commits and per-key queues, are small and are written in the driver against the TCK.

### D2. Kafka 4.0 and later, consumer protocol only
4.0 is KRaft-only and has the KIP-848 consumer group protocol. That gives one rebalance path to build and to test under chaos, with no ZooKeeper code.
- **Supporting 3.9 was rejected.** It needs the classic protocol beside the consumer protocol, which means two rebalance paths and double the chaos matrix, for clusters that are leaving support.

Verify reads the cluster's version and features and refuses older clusters. CI runs `apache/kafka` at 4.0.x and the newest release.

### D3. The resource model
| Kind id | Class | Label | Notes |
|---|---|---|---|
| `kafka.topic` | DESTINATION | Topic / Topics | Configs are config attributes, through `describeConfigs` and `incrementalAlterConfigs` |
| `kafka.partition` | PARTITION | Partition / Partitions | Leader, replicas, ISR, log start and end, size from `describeLogDirs` |
| `kafka.group` | CONSUMER_GROUP | Group / Groups | Type (classic, consumer, share, streams), state, members, assignment, from `listGroups` and describe |
| `kafka.group-topic` | SUBSCRIPTION | Group on topic / Groups on topics | Consumer-group subscription; backlog = lag |
| `kafka.share-group-topic` | SUBSCRIPTION | Share group on topic / Share groups on topics | Declared only when `share.version` ≥ 1 |
| `kafka.acl` | ACCESS_RULE | ACL / ACLs | `describeAcls`, create, delete |
| `kafka.transactional-id` | TRANSACTION | Transactional ID / Transactional IDs | List, describe, abort a hanging transaction |

Brokers are nodes. The controller quorum (`describeMetadataQuorum`) and under-replicated partitions feed health. Streams groups are listed read-only, because their offsets belong to the Streams application.

### D4. Lag is an estimate
Lag is the end offset minus the committed offset, summed per subscription. A group's isolation level is a client setting that Admin cannot see, so the end offset is always the high watermark. It is always marked an estimate:
- end offsets move while they are read;
- transaction markers and compaction make offset arithmetic overcount.

A partition with no committed offset is "unavailable" with the reason, never zero. A share group's backlog uses the share-group offsets Admin reports.

### D5. Honest destructive operations
- **Purge.** `alterConsumerGroupOffsets` to `OffsetSpec.latest()` for that group only.
- **Seek.** The same call to `earliest`, `forTimestamp(t)`, or an explicit offset.

Kafka allows both only on an empty group. The preview lists active members, and the run re-checks under the preview's etag and fails `FAILED_PRECONDITION` rather than kicking members.
- **Rejected: removing members first.** Forcibly removing members (`removeMembersFromConsumerGroup`) would change running applications as a side effect. That is a separate, explicit operation if ever wanted.

- **Truncate.** `deleteRecords` before a position per partition. The preview computes, per group, the unread records lost (committed offset < new log start). Compact-only topics are refused in preview, before Kafka raises `PolicyViolationException`.

The etag of a preview hashes the group's generation and committed offsets, or the partitions' log-start offsets, so a run after a change is refused.

### D6. Browse without a group
An assigned `KafkaConsumer` with no `group.id`, `enable.auto.commit=false`, and `seek` or `offsetsForTimes`, bounded by count and bytes. Read-committed subscriptions browse with `isolation.level=read_committed`.

### D7. Offsets as settlement, per-key lanes
Each assigned partition has a settlement tracker that holds every offset from the moment it is fetched (buffered, queued in a lane, paused or running) until it is settled. The commit position is the lowest offset not yet settled, or one past the highest offset fetched when every fetched offset is settled. A record that is fetched but waiting is never committed past. After a revoke, records settled beyond the first unsettled one may be delivered again; this is in the stated duplicate contract. Commits are asynchronous on a cadence, and synchronous on revoke and close.

Ordering:
- `PARTITION`: one lane per partition.
- `GROUP`: lanes keyed by record key within a partition. Records without a key are their own group.
- `NONE`: any lane.
- `SUBSCRIPTION`: only on a one-partition topic, checked at open.

Credit is the processor's unsettled bound. When it is reached, the consumer pauses its partitions and keeps polling (so it stays in the group), and resumes them as credit returns. `max.poll.records` and the fetch byte limits give the bytes-based prefetch bound. Zero prefetch is not offered, because the consumer always fetches ahead.

### D8. Rebalances
The consumer protocol is incremental. `onPartitionsRevoked`:
- stops dispatch for those partitions;
- waits for in-flight handlers up to the grace time;
- commits synchronously;
- hands back the rest uncounted.

The callback runs on the poll thread, so the grace time must be shorter than the rebalance timeout. Otherwise the member is fenced mid-callback and the clean revoke becomes the lost path. A longer grace time is refused at open.

`onPartitionsLost` marks the partition's in-flight messages stale, so their settles fail `DELIVERY_STALE`. Polling runs on one platform thread per consumer, as the client needs. Handlers run on virtual threads, and no consumer call is made from a handler thread.

### D9. Guarantees and transactions
| Guarantee | Producer | Consumer |
|---|---|---|
| At most once | `acks=1` | commit before dispatch |
| At least once | `acks=all`, idempotence on | commit after settlement |
| Transacted | `transactional.id` per processor lane set, `acks=all` | `isolation.level=read_committed`; `sendOffsetsToTransaction` with the consumer's group metadata |

At-least-once and transacted sends require the destination topic to be durable. A topic with `min.insync.replicas` of 1 is reported in the topic's status as weak durability, but it is not refused: that is the operator's choice.

Joined transactions are declared. A transacted processor's sender joins the lane set's producer transaction.

Each processor instance has one stable `transactional.id` (a configured prefix and an instance id). Zombie fencing goes through `sendOffsetsToTransaction(offsets, consumer.groupMetadata())` (KIP-447), not one producer per partition.
- **Rejected: ids derived from the partition set.** With incremental rebalances the set changes constantly, so the ids would churn.

### D10. Idempotence scope (SDK change)
Kafka's idempotent producer deduplicates its own retries within a producer session. It has no broker-side deduplication of caller ids.
- **Rejected: declaring idempotence unavailable.** Today's binary declaration would force this, and then ordering at least once (which needs idempotence) would be refused on the system where ordering matters most.
- **Rejected: declaring it available.** That would promise caller-id deduplication Kafka cannot keep.

The SDK gains a `DeduplicationScope` (`NONE`, `SESSION`, `WINDOW`) in `TransportCapabilities`, and a sender may ask for a minimum scope:
- Artemis declares `WINDOW`, with its duplicate-id cache as the window.
- Kafka declares `SESSION`.

`SendReceipt.resent` is true only for the driver's own re-sends after a producer reset. The client's internal retries are invisible and deduplicated.

### D11. Retry and dead letters (SDK change for the default)
A Kafka consumer group has no system retry. Under today's default (`RetryMode.SYSTEM`) a default processor would be refused on Kafka, which breaks the "defaults are usable" scenario.
- **Rejected: emulating "system" in the client.** That would hide where enforcement happens, which ADR 0014 made explicit.

The default instead becomes the transport's declared enforcement, per subscription kind:
- system where the system enforces;
- managed otherwise.

In code, `RetryPolicy.defaults()` stops naming a mode, and the processor resolves it against `TransportCapabilities` at open.

On a consumer group:
- **Ordered** processing retries in place: pause the partition, back off, re-dispatch. This needs no seek, because the record is still held.
- **Unordered** processing releases through one retry topic per backoff step, `<topic>.retry-<n>`, as Spring for Apache Kafka does. Every record in a topic then has the same delay, and the head is always due first. Headers carry the attempt and the due time, and a retry consumer in the same processor pauses each retry partition until its head is due.
  - **Rejected: a single retry topic with mixed due times.** A late record at the head would block earlier-due ones behind it.
  - **Rejected: holding failed records in memory.** It blocks the commit position for the whole backoff, and the record is lost on a crash before the first retry is due.
- **Dead letter** goes to the policy's target, or `<topic>.dlt` by default, with the headers `openbroker.origin.topic`, `.partition`, `.offset`, `.attempts` and `.reason`. In a transacted processor, the dead-letter send and the offset commit are one transaction.

Retry and dead-letter topics are created on demand when the account may create topics. Otherwise the open is refused, with the topic named.

### D12. Share groups as their own subscription kind
- **Rejected: a flag on the processor.** A share group and a consumer group are different groups on the broker, with different semantics. A processor opened on a `kafka.share-group-topic` subscription uses `KafkaShareConsumer` in explicit acknowledgement mode.

The mapping:
- accept → `ACCEPT`;
- release → `RELEASE`;
- reject → `REJECT`.

The system retry policy is declared, enforced by `group.share.delivery.count.limit` and the lock duration. System dead-lettering is declared when the cluster supports share-group dead-letter queues; the Kafka docs tie them to `share.version` ≥ 2, which the apply session re-checks with ctx7 for the release that ships them. Otherwise it is unavailable with a remedy, and a managed policy dead-letters by sending to `<topic>.dlt` and then rejecting. Purge and seek on a share group use `alterShareGroupOffsets`, under the same empty-group rule as consumer groups. A managed policy is refused when the broker's delivery limit would cut it short (ADR 0014's rule). Ordering is `NONE` only. Credit maps to `max.poll.records` and to not polling while full, which the broker's acquisition lock bounds.

### D13. Credentials (SDK change)
`Credentials` becomes a sealed interface:
- `UsernamePassword(username, Secret password)`, which Artemis uses;
- `BearerToken(Secret token, Instant expiresAt)`.

The Kafka driver installs its own SASL callback handler (PLAIN, SCRAM-SHA-256/512, OAUTHBEARER) that asks the `CredentialSource` on every authentication. Mutual TLS uses an `ssl.engine.factory.class` that reads the SDK's `TlsProvider` on every new connection. Rotation needs no restart, and hostname verification stays on.

### D14. Kind labels (SDK change)
`Kind` gains `label` (singular, plural), which every driver fills. The TCK fails a blank label. Consumers stop deriving labels from ids, because "group-topic" has no good derived label.

### D15. Chaos fixture (SDK change)
`TckChaos` names `serving()` and `successor()` nodes for a given link, in place of `primary()` and `backup()`:
- Artemis maps them to the HA pair.
- Kafka maps them to the partition leader and an in-sync follower, on a three-broker KRaft cluster (RF 3, `min.insync.replicas` 2, unclean leader election off).

Network cuts use Toxiproxy per broker, with the advertised listeners pointed at the proxies. Kills use Docker, as Artemis does. A three-broker KRaft cluster is built from three `KafkaContainer`s on one network, with combined controller and broker roles.

### D16. Notifications
Kafka emits no events. The support library's polling watch diffs topics, groups and partitions on an interval, and the overflow contract is the TCK's.

### D17. ADRs
- 0018 the Kafka mapping and its honest destructive operations (D3-D6);
- 0019 offsets as settlement and per-key lanes (D7, D8);
- 0020 deduplication scope (D10);
- 0021 the retry default follows enforcement, with retry and dead-letter topics (D11);
- 0022 share groups as a subscription kind (D12);
- 0023 credential forms (D13);
- 0024 kind labels (D14);
- 0025 the chaos fixture names the serving node and its successor (D15).

## Risks / Trade-offs

- [The three-broker cluster with Toxiproxy is slow and memory-hungry on CI] → The chaos job keeps its own matrix entry, and the brokers run with small heaps (`KAFKA_HEAP_OPTS`).
- [Retry topics multiply topics on busy clusters] → They are created only for unordered managed policies. Their names are listed in the processor's status, and an explicit target can replace them.
- [Contiguous-commit tracking holds memory per in-flight offset] → It is bounded by the processor's unsettled credit.
- [Lag arithmetic misleads on compacted or transactional topics] → It is always an estimate, with the reason in the total's metadata.
- [Breaking the API for Artemis consumers] → `0.3.0`, with migrations in the `BREAKING CHANGE:` footers.

## Migration Plan

Consumers of `0.2.0` change three things:
- `Credentials` construction becomes `Credentials.usernamePassword(...)`;
- the chaos fixture methods are renamed;
- code that relied on the `SYSTEM` default reads the resolved mode from the processor.

Rollback is the previous release.
