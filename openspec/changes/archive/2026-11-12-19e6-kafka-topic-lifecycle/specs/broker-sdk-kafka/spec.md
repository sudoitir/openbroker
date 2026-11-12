## ADDED Requirements

### Requirement: Creating a topic is previewed and refuses an internal name

The driver SHALL preview creation of a topic as the topic that will exist: its name, its partition count, its replication factor and its settings. The preview SHALL ask the broker to validate the topic without creating it, and SHALL say that nothing has been created. Creating a topic whose name begins with `__`, or that the cluster marks internal, SHALL be refused with the reason that the cluster manages internal topics, and SHALL NOT call the broker to create or delete it. A partition count below 1 SHALL be refused. A replication factor below 1, or above the number of brokers the connection reached, SHALL be refused, and the refusal SHALL name how many brokers were reached. A setting the topic kind does not carry SHALL be refused in the preview. On apply, a refusal from the broker SHALL leave no topic.

#### Scenario: Creating a topic

- **WHEN** a caller previews creation of a topic with 6 partitions and replication factor 3, and three brokers were reached
- **THEN** the preview states that name, partition count, replication factor and settings, and applying it creates that one topic

#### Scenario: An internal name

- **WHEN** a caller previews creation of a topic named `__consumer_offsets`
- **THEN** the preview is refused with the reason that the cluster manages internal topics, and no topic is created

#### Scenario: Replication factor above the brokers reached

- **WHEN** a caller previews a replication factor of 3 and the connection reached 2 brokers
- **THEN** the preview is refused and names that 2 brokers were reached

#### Scenario: The broker rejects the topic

- **WHEN** the broker rejects the create
- **THEN** the caller receives the broker's reason and the topic does not exist

### Requirement: Deleting a topic names who still reads it

The driver SHALL preview deletion of a topic with an estimate of the records it holds (each partition's end offset minus its log start, summed) and the same estimate condition a lag uses, and SHALL name every group that still has offsets on that topic. The preview SHALL say that deleting the topic does not erase those groups' offsets, and that the groups will show the topic as deleted. An internal topic SHALL be refused with the reason that the cluster manages internal topics, and SHALL NOT be deleted. A request that the topic must be empty SHALL be refused when the estimate is above zero, naming the estimate. The preview SHALL be stamped with an etag over the topic's identity and partition count, and a delete whose etag no longer matches SHALL be refused and change nothing.

#### Scenario: Groups still have offsets

- **WHEN** a caller previews deletion of a topic that two groups still have offsets on
- **THEN** the preview names both groups, gives the record estimate as an estimate, and says those groups will show the topic as deleted

#### Scenario: An internal topic

- **WHEN** a caller previews deletion of a topic the cluster marks internal
- **THEN** the preview is refused with the reason that the cluster manages internal topics, and the topic remains

#### Scenario: Delete only if empty

- **WHEN** a caller asks to delete a topic only if it is empty, and the record estimate is above zero
- **THEN** the delete is refused and names the estimate, and the topic remains

#### Scenario: The topic changed after the preview

- **WHEN** the topic's partition count changes between the preview and the delete
- **THEN** the delete is refused as stale and the topic remains

### Requirement: Increasing partitions is validated before it is applied

The driver SHALL preview an increase of a topic's partition count by asking the broker to validate the new count without creating partitions. The preview SHALL show the current count and the new count, and SHALL say that records already written stay on their partitions and that a key can land on a new partition afterwards. A count lower than the topic's current count SHALL be refused, naming both counts, and SHALL NOT call the broker. A replica assignment SHALL be refused; placement is not part of this operation. The preview SHALL carry an etag over the current partition count, and an apply whose etag no longer matches SHALL be refused and change nothing. A count the broker rejects SHALL be shown on the preview, and nothing SHALL be applied.

#### Scenario: Six partitions become twelve

- **WHEN** a caller previews raising a topic from 6 partitions to 12 and then applies that preview
- **THEN** the preview shows 6 and 12 and the note about existing records and keys, the broker is asked to validate before anything is created, and the topic then has 12 partitions

#### Scenario: A shrink

- **WHEN** a caller previews lowering a topic from 12 partitions to 6
- **THEN** the preview is refused, names both counts, and the topic still has 12 partitions

#### Scenario: The broker rejects the count

- **WHEN** the broker rejects the validated count
- **THEN** the preview shows the broker's message and no partitions are added

### Requirement: Deleting a group never removes its members

The driver SHALL preview deletion of a group with its state and its members. When the group has members, or is rebalancing, the preview SHALL name the members and the delete SHALL be refused, and the driver SHALL NOT remove those members. A streams group SHALL stay read-only with the existing reason, and deletion SHALL be refused with that reason. A share group SHALL be deleted only where share groups are declared for the cluster, and only when it has no members. The preview SHALL carry an etag over the group's state, epoch and members, and a delete whose etag no longer matches SHALL be refused and change nothing.

#### Scenario: An empty group

- **WHEN** a caller previews deletion of an empty group and applies it
- **THEN** the preview shows the state and that there are no members, and the group is deleted

#### Scenario: A group with members

- **WHEN** a caller previews deletion of a group with two members
- **THEN** the preview names both members, the delete is refused, and both members are still in the group

#### Scenario: A streams group

- **WHEN** a caller previews deletion of a streams group
- **THEN** the delete is refused with the reason that the streams application owns the group
