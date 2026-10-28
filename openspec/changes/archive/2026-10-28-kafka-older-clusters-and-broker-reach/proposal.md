## Why
The Kafka driver supports Kafka 4.0 and later, but most running estates are on Kafka 3.x, often in ZooKeeper mode, which the 4.x clients speak fully. Classic groups, still the default protocol of most clients, are read-only, so an empty, idle one cannot be rewound although Kafka allows it. And `verify` checks the cluster through whichever broker answers, so a broker that advertises an address the caller cannot reach passes the check and fails later.

## What Changes
- The Kafka driver supports Kafka 3.0 and later, in ZooKeeper and KRaft mode. What a cluster lacks (the KRaft quorum, consumer-protocol groups, share groups, streams groups, fenced brokers, eligible leader replicas) is declared unavailable per feature with its reason, never emulated. A cluster before 3.0 is refused at verify.
- Empty, idle classic groups become subscriptions: purge and seek work on them, refused while the group has members or is rebalancing. Streams groups, and classic groups run by Kafka Streams, stay read-only. Supersedes the classic-group point of ADR-0018 (ADR-0031).
- The messaging client uses the classic group protocol where the cluster lacks the consumer protocol.
- `verify` reaches every broker on the address it advertises and names each broker's outcome: an address that does not resolve, a refused or timed-out connection, a TLS or authentication failure.
- `verify` names every node's outcome in the SDK's partial result, and the TCK checks it, including with a node cut.
- The Kafka CI matrix gains a ZooKeeper-mode Kafka 3.0 leg and Kafka 3.9.

## Capabilities
### New Capabilities
None.

### Modified Capabilities
- `broker-sdk-kafka`: the supported range, classic groups as subscriptions, the protocol the messaging client picks, declared gaps and reach in verify.
- `broker-sdk`: verify names every node's outcome.

## Out of scope
- Kafka before 3.0.
- Resetting streams groups, or removing a group's members to reset it.
- Emulating any feature a cluster lacks.

## Impact
`openbroker-kafka` (`KafkaDriver`, `admin`: `ClusterFeatures` replaces `ClusterVersions`, `Reach`, `Groups`, `Subscriptions`, `KafkaNodes`, `KafkaAccess`; `messaging`: `Consumption`); `openbroker-tck` (`OpenBrokerTck`, `ChaosTck`); test fixtures and the CI matrix; README; ADR-0031. No change to `openbroker-api`.
