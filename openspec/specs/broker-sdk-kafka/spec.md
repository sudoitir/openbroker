# broker-sdk-kafka Specification

## Purpose
Defines the Apache Kafka driver: how Kafka's topics, partitions and groups map onto the SDK's resource model, how its destructive operations state their reach, how the messaging client runs on consumer groups and share groups, and how the driver proves it against the TCK.

## Requirements

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

### Requirement: A subscription's backlog is its lag, as an estimate
A subscription's backlog SHALL be reported as an estimate with a per-partition breakdown. For a consumer group it SHALL be the partition's end offset minus the committed offset, and the estimate's reason SHALL say that transaction markers and compaction can make it overcount. For a share group it SHALL be the records not yet acknowledged.

#### Scenario: Lag is shown per partition
- **WHEN** a group lags on a topic with six partitions
- **THEN** the subscription's backlog is the summed lag, marked as an estimate, with each partition's share

#### Scenario: A partition with no committed offset
- **WHEN** a group has never committed on one partition of a topic it reads
- **THEN** that partition's lag is reported as unavailable with the reason, and the total is still marked as an estimate

### Requirement: Per-message operations are declared unavailable
The driver SHALL declare per-message move, copy and delete unavailable on every kind, each with the reason that Kafka's log does not delete single records. It SHALL never emulate them by copying records and resetting offsets.

#### Scenario: Moving a message is not offered
- **WHEN** a consumer checks whether a Kafka message can be moved or deleted
- **THEN** both are unavailable, with the reason

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

### Requirement: Share-group subscriptions reset through the broker
Purging and seeking a share-group subscription SHALL move the share group's start offsets in the same ways and under the same rule: only while the group has no active members, named in the preview, against the preview's etag.

#### Scenario: Purging a share group
- **WHEN** an operator purges an empty share group's subscription
- **THEN** its start offset on each partition moves to the end, and no other group changes

### Requirement: Truncate states every group it affects
Truncating a topic or a partition SHALL delete records before a position for every group. Its preview SHALL name every group that loses unread records, with how many, and the run SHALL use the preview's etag. A topic whose cleanup policy is compaction without deletion SHALL be refused in preview with `FAILED_PRECONDITION`.

#### Scenario: Truncate warns about other groups
- **WHEN** an operator previews truncating a topic that three groups read
- **THEN** the preview says the data is removed for all three groups and how many unread records each loses

#### Scenario: Truncating a compacted topic
- **WHEN** an operator previews truncating a topic whose only cleanup policy is compaction
- **THEN** the preview fails with `FAILED_PRECONDITION`, naming the cleanup policy

### Requirement: Browsing never changes a group
Browsing SHALL read a topic by partition from an offset or a time, without joining a group or committing an offset. A browse SHALL be bounded by count and bytes. Over a read-committed subscription, it SHALL show only committed records.

#### Scenario: Browsing from a time
- **WHEN** an operator browses partition 2 of a topic from a time
- **THEN** the records from the first offset at or after that time are returned, and no group's offsets or membership change

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

### Requirement: Rebalances follow the loss and duplicate contract
On a cooperative rebalance, the driver SHALL let handlers of revoked partitions finish within the processor's grace time, commit what they settled, and hand the rest back uncounted. When partitions are lost without revocation, settling a record of a lost partition SHALL fail with `DELIVERY_STALE`, and the record SHALL be delivered again to the partition's new owner.

#### Scenario: A member joins
- **WHEN** a second processor joins a group while the first is handling records of a partition that moves
- **THEN** the first finishes or hands back those records, the second resumes at the committed offset, no record before it is handled again, and only records settled after the first unsettled one may be delivered again

#### Scenario: A member is cut off
- **WHEN** a processor's network is cut past the session timeout while records are in flight
- **THEN** its later settles fail with `DELIVERY_STALE`, and the new owner receives those records

### Requirement: Grace fits inside the rebalance timeout
A processor's grace time SHALL be shorter than the group's rebalance timeout, and a longer one SHALL be refused at open with `INVALID_ARGUMENT`.

#### Scenario: A grace time longer than the rebalance timeout
- **WHEN** a processor opens with a grace time longer than the group's rebalance timeout
- **THEN** it is refused with `INVALID_ARGUMENT`, naming both durations

### Requirement: Kafka's guarantees are declared
The driver SHALL declare at most once (offsets committed before a record is handed over), at least once (every send acknowledged by all in-sync replicas, offsets committed after settlement) and transacted (records sent and offsets committed in one Kafka transaction, reading only committed records). It SHALL declare joined transactions, so that a transacted processor's sends and settled offsets commit together.

#### Scenario: Consume, transform, produce
- **WHEN** a transacted processor sends a transformed record for each record it receives and the transaction is aborted
- **THEN** neither the sends are visible to read-committed readers nor the offsets committed, and the records are delivered again

### Requirement: Idempotence has the producer-session scope
The driver SHALL declare idempotent sending with the session scope: the client's own re-sends are deduplicated, and caller-supplied ids are not deduplicated across sessions.

#### Scenario: Idempotence with a caller id
- **WHEN** a sender asks for deduplication by caller id across sessions
- **THEN** it is refused with `UNIMPLEMENTED`, naming the session scope the driver offers

### Requirement: Consumer-group retries are client-enforced
For a consumer group, the driver SHALL declare that the system enforces none of delay, attempts and dead-lettering. The default policy SHALL resolve to a managed policy that retries in place and creates no retry topic. An ordered processor SHALL retry in place, holding its partition and keeping order. An explicitly managed, unordered processor SHALL release a failed record through a retry topic per backoff step, carrying its attempt count and due time, so that the partition keeps moving.

#### Scenario: An unordered retry does not block the partition
- **WHEN** an unordered processor's handler fails once on a record and later records of its partition succeed
- **THEN** the later records are handled and committed while the failed record waits its backoff in the retry topic, then is handled again

### Requirement: A missing retry or dead-letter topic refuses the open
A retry or dead-letter topic that does not exist and that the account may not create SHALL refuse the processor's open with `FAILED_PRECONDITION`, naming the topic and the remedy, before the group is joined.

#### Scenario: A default processor that cannot create its dead-letter topic
- **WHEN** a processor opens with default options on a consumer group, its topic's dead-letter topic does not exist, and the account may not create topics
- **THEN** it is refused with `FAILED_PRECONDITION`, naming the dead-letter topic and how to create it or name another, and no group is joined

### Requirement: Dead letters keep their origin
After the last attempt, or on a rejection, a consumer-group record SHALL be sent to the policy's dead-letter topic with its origin topic, partition, offset, attempt count and reason as headers, before its offset is committed.

#### Scenario: Dead-lettering keeps the origin
- **WHEN** an unordered processor rejects a record from partition 3, offset 120
- **THEN** the dead-letter topic holds the record with headers naming the origin topic, partition 3, offset 120 and the reason, and the group's offset moves past it

### Requirement: Share groups are a queue subscription
On a cluster where share groups are enabled at a production feature level, the driver SHALL offer a share-group subscription kind, declared from the cluster's features at connect. A processor on it SHALL be unordered, SHALL settle each record by accepting, releasing or rejecting it, and SHALL use the broker's delivery count for the system retry policy. It SHALL renew a record's acquisition lock while its handler runs. Any ordering but none SHALL be refused with `UNIMPLEMENTED`.

#### Scenario: Release and redeliver
- **WHEN** a handler releases a share-group record
- **THEN** the record is delivered again, possibly to another member, with its delivery count raised

#### Scenario: A cluster without share groups
- **WHEN** a consumer lists the kinds of a cluster where share groups are not enabled
- **THEN** the share-group subscription kind is unavailable, with the reason and the feature that enables it as the remedy

### Requirement: Share groups dead-letter through the broker
System dead-lettering SHALL be declared only where the cluster's `share.version` feature is at level 2 or higher (Kafka 4.4.0 and later). There, a rejected share-group record, or one past its delivery limit, SHALL go to the group's dead-letter topic. Elsewhere, system dead-lettering SHALL be declared unavailable with the reason and the feature that enables it, and a managed policy SHALL dead-letter by sending the record to its dead-letter topic before rejecting it.

#### Scenario: A record past its delivery limit
- **WHEN** a share-group record is released more times than the group's delivery limit on a cluster with share-group dead-lettering
- **THEN** the broker moves it to the dead-letter topic, and it is not delivered again

#### Scenario: A cluster without share-group dead-lettering
- **WHEN** a processor opens with a managed policy on a share-group subscription of a cluster whose `share.version` is below 2, and a record fails every attempt
- **THEN** system dead-lettering is reported unavailable with the feature as the remedy, and the record is sent to the policy's dead-letter topic and then rejected

### Requirement: System dead-lettering needs a configured topic
A processor with the system policy on a share group SHALL be refused at open with `FAILED_PRECONDITION` when the cluster dead-letters share groups but the group names no dead-letter topic, with a remedy naming the group and topic configs.

#### Scenario: A system policy on a group with no dead-letter topic
- **WHEN** a processor opens with the system policy on a share group of a cluster with share-group dead-lettering, but the group names no dead-letter topic
- **THEN** it is refused with `FAILED_PRECONDITION`, and the remedy names the group config for the dead-letter topic

### Requirement: Kafka messaging is secure
The driver SHALL authenticate with SASL PLAIN, SASL SCRAM, SASL OAUTHBEARER and mutual TLS. It SHALL ask for credentials and TLS material on every new connection to a broker, so that rotation needs no restart. Hostname verification SHALL stay on. A topic the account may not use SHALL fail with `PERMISSION_DENIED`, naming the topic, without closing the client.

#### Scenario: Credentials rotate under load
- **WHEN** the credential source returns a new SCRAM password while a processor runs
- **THEN** new broker connections use it, and the processor keeps running

### Requirement: The Kafka driver passes the TCK
The driver SHALL pass every TCK, messaging TCK and chaos TCK case for the capabilities it declares, against a real cluster of the oldest and newest supported Kafka, in ZooKeeper mode on the oldest and KRaft on the newest, and SHALL prove share-group dead-lettering against a cluster that supports it. The chaos cases SHALL kill a partition's leader mid-flight, cut the network to it, and check that at least once loses no acknowledged record with three replicas and two in sync required.

#### Scenario: A leader is killed mid-flight
- **WHEN** the leader of a partition is killed while an at-least-once sender and processor are running
- **THEN** every record the sender confirmed is handled at least once, and the duplicates stay within the stated contract

#### Scenario: The oldest supported cluster
- **WHEN** the TCK runs against Kafka 3.0 in ZooKeeper mode
- **THEN** every case for the capabilities the driver declares there passes, and the gaps are declared unavailable with their reasons

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

### Requirement: Reads are batched per page
The driver SHALL serve a page of resources, and a read of several queries, with a number of Admin rounds that does not grow with the number of groups or topics in it: the groups' descriptions and offsets, the topics' partitions and their log offsets are each asked once for everything the page or the queries name. A query that fails SHALL fail alone, with every other query answered.

#### Scenario: A page of many subscriptions
- **WHEN** a consumer reads one query for each of twelve subscriptions, each naming its own group and topic
- **THEN** the driver asks for the group listing, the group descriptions, the groups' offsets, the topics' partitions and the log offsets in as many rounds as for two queries

#### Scenario: One query fails
- **WHEN** one of several queries names a topic that does not exist
- **THEN** the other queries are answered, and the failing query reports its own result
