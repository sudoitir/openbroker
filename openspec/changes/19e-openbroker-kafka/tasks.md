Rules for every unit:
- Wrap each heavy command in `systemd-run --user --scope -q -p MemoryMax=3G -p MemorySwapMax=2G -- …`, with at most two at once.
- Read narrowly: `grep -n` first, then only the lines needed.
- Format with `./mvnw process-sources`.
- Commits follow `.claude/rules/05-commits.md`, with `!` and a `BREAKING CHANGE:` footer on API commits.
- Library versions are looked up with ctx7 when a dependency is added.

Order and parallelism:
- **Unit 1 runs first, alone**, after the `planner` has reviewed D10-D15 against the code.
- **Units 2 and 3 run in parallel after unit 1.** They are file-disjoint: `admin` against `messaging` packages of the driver.
- **Unit 4 runs after unit 3.**
- **Unit 5 runs after units 2 and 4.**
- **Unit 6 runs last.**

## 1. SDK changes and build skeleton (files: openbroker-api/**, openbroker-tck/**, openbroker-artemis/**, poms, BOM)
- [ ] 1.1 Raise every pom to `0.3.0-SNAPSHOT`, set `openbroker.previous.version` to `0.2.0`, add the `openbroker-kafka` module and BOM entry, and manage `kafka-clients` and `testcontainers-kafka`. Check: `./mvnw -B -DskipTests verify` builds
- [ ] 1.2 `Kind.label` (singular, plural) with validation; Artemis fills every label; a TCK case fails a blank label. Check: `ArtemisTckIT` green
- [ ] 1.3 Sealed `Credentials` (`UsernamePassword`, `BearerToken`) and the `INVALID_ARGUMENT` refusal of an unsupported form; Artemis accepts only `UsernamePassword`; redaction tests for both. Check: api unit tests and `ArtemisTckIT`
- [ ] 1.4 `DeduplicationScope` in `TransportCapabilities` and a minimum scope on `SenderOptions`; Artemis declares `WINDOW`; `MessagingTck` gains the scope cases. Check: `ArtemisMessagingTckIT`
- [ ] 1.5 Retry enforcement declared per subscription kind; the default `RetryPolicy` resolves against the transport at open; a refusal for a system policy the system does not enforce. Check: the "defaults are usable" case passes on Artemis
- [ ] 1.6 `TckChaos` names `serving()` and `successor()`; `ArtemisChaosIT` maps them to the HA pair. Check: `-Pchaos -Dit.test=ArtemisChaosIT`
- [ ] 1.7 ADRs 0020, 0021, 0023, 0024 and 0025. Check: japicmp `api-check` reports the break and the minor bump

## 2. Kafka management surface (files: openbroker-kafka/src/**/kafka/admin/**, driver entry, test cluster fixture)
- [ ] 2.1 `KafkaDriver` (`system` `kafka`, `[4.0.0,)`, option schema for security protocol, SASL mechanism and client id), with verify refusing older clusters, nodes from `describeCluster` and quorum health. Check: unit tests and a `KafkaContainer` IT
- [ ] 2.2 A three-broker KRaft test cluster fixture in the driver's tests (three `KafkaContainer`s, combined roles, RF 3, `min.insync.replicas` 2). Check: it starts in CI on 4.0.x and the newest image
- [ ] 2.3 Kinds and resources: topics (CRUD, partition increase, configs), partitions, groups (all types), group-topic and share-group-topic subscriptions, ACLs and transactional ids, each with its label. Check: `KafkaTckIT extends OpenBrokerTck` for these kinds
- [ ] 2.4 Lag as an estimate with per-partition breakdown, and unavailable partitions with a reason. Check: IT with six partitions and one never-committed partition
- [ ] 2.5 Purge and seek (latest, earliest, time, offset) with an empty-group precondition, a member list in the preview and an etag over generation and offsets. Check: TCK precondition cases and an active-group IT
- [ ] 2.6 Truncate with a per-group loss preview and the compact-only refusal. Check: IT with three groups and a compacted topic
- [ ] 2.7 Browse by partition, offset and time with no group, bounded by count and bytes; per-message move, copy and delete unavailable with the reason. Check: TCK browse cases, and an IT asserting no group appears
- [ ] 2.8 Abort a hanging transaction; the polling watch for topics, groups and partitions. Check: TCK notification cases
- [ ] 2.9 ADR 0018. Check: `KafkaTckIT` green

## 3. Consumer-group messaging (files: openbroker-kafka/src/**/kafka/messaging/** except share)
- [ ] 3.1 Sender: guarantees mapped to `acks` and idempotence, bounded `send` and `offer`, `SESSION` dedup scope, `resent` on driver re-sends. Check: `MessagingTck` send cases
- [ ] 3.2 Receiver and processor: settlement tracker with contiguous commits, lanes per partition and per key, credit by pause and resume, the whole-subscription check. Check: ordering and flow-control cases, and a unit test of the tracker
- [ ] 3.3 Rebalance handling: revoke drains within grace and commits, lost marks stale (`DELIVERY_STALE`). Check: join and cut-off ITs
- [ ] 3.4 Transacted guarantee and joined transactions through `sendOffsetsToTransaction`, with fenced transactional ids. Check: the consume-transform-produce case
- [ ] 3.5 SASL PLAIN, SCRAM and OAUTHBEARER callback handlers, and the TLS engine factory over `TlsProvider`. Check: rotation and permission-denied cases against a SASL-enabled container
- [ ] 3.6 ADR 0019. Check: `KafkaMessagingTckIT extends MessagingTck` green for consumer groups

## 4. Retry, dead letters and share groups (files: openbroker-kafka/src/**/kafka/messaging/retry/**, …/share/**)
- [ ] 4.1 In-place retry for ordered processing, and retry topics with attempt and due-time headers for unordered. Check: "a message that keeps failing" and "ordered retries keep the order" on Kafka
- [ ] 4.2 Dead-letter topic with origin headers, transactional with the commit when transacted, and a refusal naming the topic when it cannot be created. Check: the dead-letter IT
- [ ] 4.3 The share-group processor on `KafkaShareConsumer` (explicit acknowledgement), the system retry by delivery count, system dead-letter when `share.version` ≥ 2, and the dynamic kind declaration. Check: release, redeliver and delivery-limit ITs on the newest image; kind unavailable on 4.0.x
- [ ] 4.4 ADR 0022. Check: `KafkaMessagingTckIT` green for both subscription kinds

## 5. Chaos, benchmarks and CI (files: openbroker-kafka/src/test/**/KafkaChaosIT*, openbroker-benchmarks/**, .github/workflows/ci.yml)
- [ ] 5.1 `KafkaChaosIT extends ChaosTck`: Toxiproxy per broker, with advertised listeners through the proxies, leader kill and cut, follower takeover. Check: `-Pchaos -Dit.test=KafkaChaosIT` green on both images
- [ ] 5.2 JMH benchmarks for the Kafka send and process paths, with their budgets in `budgets.json`. Check: the `bench` job passes
- [ ] 5.3 CI: Kafka entries in the `verify` and `chaos` matrices (4.0.x and the newest `apache/kafka`), still in `ci-ok`. Check: the PR's CI is green

## 6. Documentation and finish
- [ ] 6.1 README: the Kafka driver, its supported range, what it declares unavailable and why, and the share-group kind. Check: the README examples compile in a doc test or are copied from an IT
- [ ] 6.2 Run the full `./mvnw verify` once, in a capped scope, before the PR. Check: green locally and in CI

## Workflow follow-up
- PR, merge on green CI, `/opsx:archive`, then ask the maintainer to run the `Release` workflow for `0.3.0`.
