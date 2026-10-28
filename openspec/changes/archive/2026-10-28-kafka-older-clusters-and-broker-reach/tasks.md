## 1. Proposal and decision
- [x] 1.1 Proposal, spec deltas, tasks and ADR-0031 (supersedes ADR-0018's classic-group point). Check: `openspec validate kafka-older-clusters-and-broker-reach --strict`

## 2. Cluster features and the 3.0 range (D1, D2)
- [x] 2.1 `ClusterFeatures` (mode, release from the `metadata.version` table copied from Kafka 4.4.0, finalized feature levels, assessments) replacing `ClusterVersions`; `KafkaCluster` caches it; range `[3.0.0,)`. Check: unit tests
- [x] 2.2 Capabilities declared per feature with reasons and remedies; `KafkaNodes` in ZooKeeper mode and the node attributes `kafka.cluster_version` and `kafka.cluster_mode`; eligible-leader attributes only where enabled. Check: unit tests
- [x] 2.3 The messaging client picks `group.protocol` from the cluster's features. Check: unit tests

## 3. Classic groups as subscriptions (D3)
- [x] 3.1 `GroupInfo.manageable()`, `Subscriptions.accepts` and `requireIdle` (members and rebalancing states refused); streams-assignor classic groups stay read-only; texts follow. Check: unit tests and the group ITs

## 4. Verify reaches every broker (D4, D5)
- [x] 4.1 `Reach` and the `KafkaConnection.verify` flow, with the failure kinds and their metadata. Check: unit tests
- [x] 4.2 `OpenBrokerTck.verifyNamesEveryNodesOutcome` and `ChaosTck.verifyReportsACutNodeAndTheRest`. Check: `ArtemisTckIT` and the Kafka TCK

## 5. Fixtures, CI and docs (D6)
- [x] 5.1 `ZooKeeperKafkaBroker`, `KafkaTestCluster` and the `zookeeper` profile, `KafkaBefore30ZooKeeperIT`. Check: `-Pzookeeper`
- [x] 5.2 The CI Kafka matrix: `cp-kafka:7.0.16` (`-Pzookeeper`), `apache/kafka` 3.9.2, 4.0.2 and 4.4.0; chaos on the newest. Check: the workflow parses
- [x] 5.3 README supported range, capability table, the ZooKeeper-below-3.3 broker-down caveat; `KafkaDriver` javadoc. Check: review

## 6. Finish line
- [x] 6.1 `./mvnw -pl openbroker-kafka -am test`, `-Pzookeeper`, `-Dkafka.image=apache/kafka:3.9.2`, and `ArtemisTckIT` pass
