## MODIFIED Requirements

### Requirement: Kafka is a driver of the SDK
The driver SHALL register the system `kafka` through `ServiceLoader`, declare Kafka 3.0 and later as its supported versions, in ZooKeeper and KRaft mode, and connect from bootstrap endpoints given as the cluster's endpoints. A cluster below 3.0 SHALL be refused at verify with `FAILED_PRECONDITION`, naming what was found. The driver SHALL depend on no framework.

#### Scenario: Connecting to a supported cluster
- **WHEN** a consumer connects with the system `kafka` and the bootstrap endpoints of a Kafka 4.x cluster
- **THEN** the connection reports every broker as a node, and the controller quorum in the cluster's health

#### Scenario: A ZooKeeper cluster at 3.0
- **WHEN** a consumer verifies a connection to a Kafka 3.0 cluster in ZooKeeper mode
- **THEN** verify succeeds, the connection description says "at least 3.0, ZooKeeper mode", and every broker is a node

#### Scenario: An unsupported cluster
- **WHEN** a consumer verifies a connection to a Kafka 2.8 cluster
- **THEN** verify fails with `FAILED_PRECONDITION`, and the reason says the cluster runs a Kafka before 3.0 and names the supported range

### Requirement: Kafka is modelled honestly
The driver SHALL map a topic to a destination, a partition to a partition, a group to a consumer group with its type (classic, consumer, share or streams) and state, a classic, consumer or share group on a topic to a subscription, an ACL to an access rule and a transactional id to a transaction. A streams group, and a classic group run by Kafka Streams, SHALL be read-only with no subscription. A topic's configuration SHALL be its config attributes. Every kind SHALL carry Kafka's own label.

#### Scenario: Native labels
- **WHEN** a consumer lists the driver's kinds
- **THEN** each kind carries Kafka's label, for example "Topic", "Group" and "Group on topic"

#### Scenario: A streams group is read-only
- **WHEN** a consumer lists the groups of a cluster that runs a Kafka Streams application
- **THEN** the streams group is listed with its type and state, offers no subscription, and purge and seek are unavailable on it with the reason that its offsets belong to the application

#### Scenario: A classic group is a subscription
- **WHEN** a consumer lists the groups of a cluster that has a classic group made by an ordinary consumer
- **THEN** the group offers a subscription on each topic it has an offset on

### Requirement: Purge and seek reach one empty group
Purging a consumer, classic or share group's subscription SHALL move its committed offsets to the end of each partition; seeking SHALL move them to the earliest, the latest, a time or a given offset. Either SHALL affect only that group and run only while it has no active members and is not rebalancing. A preview SHALL name the members; a run against a group with members or in a rebalance SHALL fail with `FAILED_PRECONDITION`, changing no offset. Both SHALL run against their preview's etag.

#### Scenario: Purging an active group
- **WHEN** an operator purges a subscription whose group has two running members
- **THEN** the preview names both members, the run fails with `FAILED_PRECONDITION`, and no offset changes

#### Scenario: Purging an empty classic group
- **WHEN** an operator purges the subscription of an empty classic group
- **THEN** the group's committed offsets move to the end of each partition, and no other group changes

#### Scenario: A group that is rebalancing
- **WHEN** an operator purges a subscription whose group has no members but is in a rebalance
- **THEN** the run fails with `FAILED_PRECONDITION`, the reason says the group is rebalancing and to try again once it settles, and no offset changes

#### Scenario: Seeking to a time
- **WHEN** an operator seeks an empty group's subscription to a time
- **THEN** each partition's committed offset becomes the first offset at or after that time, or the end where none is

### Requirement: Consumer groups settle by offset
A processor on a consumer-group subscription SHALL settle each record independently and commit a partition's offset only to the record after the highest contiguous settled record, never past an unsettled one. It SHALL use the consumer protocol where the cluster has it and Kafka's classic protocol where it does not. Ordering per partition and per key SHALL be supported; ordering of the whole subscription only on a single-partition topic, else `UNIMPLEMENTED`.

#### Scenario: Out-of-order settlement
- **WHEN** a processor with four lanes settles records 5, 7 and 6 of one partition in that order
- **THEN** the committed offset is at most 6 after record 5, and 8 only after record 6 settles

#### Scenario: Ordered per key
- **WHEN** a processor ordered per group handles interleaved keys of one partition in four lanes
- **THEN** each key's records are handled in offset order, one at a time, and different keys run concurrently

#### Scenario: A cluster without the consumer protocol
- **WHEN** a processor opens a subscription on a Kafka 3.x cluster
- **THEN** it joins with the classic group protocol and settles by offset as on any other cluster

### Requirement: The Kafka driver passes the TCK
The driver SHALL pass every TCK, messaging TCK and chaos TCK case for the capabilities it declares, against a real cluster of the oldest and newest supported Kafka, in ZooKeeper mode on the oldest and KRaft on the newest, and SHALL prove share-group dead-lettering against a cluster that supports it. The chaos cases SHALL kill a partition's leader mid-flight, cut the network to it, and check that at least once loses no acknowledged record with three replicas and two in sync required.

#### Scenario: A leader is killed mid-flight
- **WHEN** the leader of a partition is killed while an at-least-once sender and processor are running
- **THEN** every record the sender confirmed is handled at least once, and the duplicates stay within the stated contract

#### Scenario: The oldest supported cluster
- **WHEN** the TCK runs against Kafka 3.0 in ZooKeeper mode
- **THEN** every case for the capabilities the driver declares there passes, and the gaps are declared unavailable with their reasons

## ADDED Requirements

### Requirement: What a cluster lacks is declared, never emulated
The driver SHALL declare each feature the cluster lacks unavailable with its reason, and where an operator can enable it, the remedy: the KRaft quorum, consumer-protocol groups, share groups, streams groups, fenced brokers and eligible leader replicas. It SHALL NOT emulate a missing feature.

#### Scenario: The quorum on a ZooKeeper cluster
- **WHEN** a consumer checks the quorum capability of a ZooKeeper cluster
- **THEN** it is unavailable, with the reason that the cluster has no KRaft controller quorum to describe

#### Scenario: Consumer-protocol groups
- **WHEN** a consumer checks consumer-protocol groups on a cluster whose `group.version` is not finalized at 1
- **THEN** they are unavailable, the reason names the level found, and on Kafka 4.x the remedy names the feature upgrade

#### Scenario: Share and streams groups
- **WHEN** a consumer checks share groups and streams groups on a cluster that lacks them
- **THEN** each is unavailable with its own reason

#### Scenario: Fenced brokers and eligible leaders
- **WHEN** a consumer checks fenced brokers on a cluster before Kafka 4.0, and eligible leader replicas on one that has not enabled them
- **THEN** each is unavailable with its reason, and the partitions carry no eligible-leader attributes

### Requirement: Verify reaches every broker on its advertised address
Verify SHALL reach each broker on the address the cluster advertises for it, within the connect timeout and the caller's deadline, and SHALL fail an unreached broker with `UNAVAILABLE`, not sent, naming the broker, its address and the cause: the address does not resolve, the connection is refused or times out, TLS fails, or the broker does not answer.

#### Scenario: An address that does not resolve
- **WHEN** a broker advertises a host name the caller cannot resolve
- **THEN** that broker is a failure naming its address and that it does not resolve, and the other brokers keep their descriptions

#### Scenario: A broker that is cut
- **WHEN** one broker of three is unreachable at verify
- **THEN** verify returns within its deadline with that broker failed and the other two applied
