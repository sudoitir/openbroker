# ADR-0036: Kafka topic and group lifecycle is previewed before it is applied

- **Status**: accepted
- **Date**: 2026-11-12
- **Deciders**: Mahdi Amirabdollahi

## Context

The Kafka driver can already create a topic, delete a topic, raise its partition count and delete a group.
Those calls did not say, before they ran, what they would do. A delete preview counted one topic. It did not
name the groups that still had offsets, and it did not label the record count as an estimate. A create was
sent with no check that the broker would accept it. A group delete removed the group's members when the
caller set `disconnectConsumers`, which is the same side effect ADR-0018 already refuses for purge and seek.

`CreateTopics` and `CreatePartitions` both accept `validateOnly`. It has been on the create-topics request
since version 1, which Kafka 2.8 speaks. A preview can ask the broker and create nothing.

## Decision

- A create preview refuses an internal name (a name starting with `__`), a partition count below 1, a
  replication factor outside `1..brokers reached` (the refusal names that count), and a setting the topic
  kind does not declare, all before any create is sent. It then calls `createTopics` with `validateOnly`.
  Apply calls it without that flag. A broker rejection leaves no topic.
- A delete preview names every group that still has offsets on the topic, estimates the records as each
  partition's end offset minus its log start, and says that deleting the topic does not erase those offsets.
  The same condition a lag uses applies: the end offset moves while it is read, and transaction markers and
  compaction can overcount. An internal topic, including one the cluster marks internal, is refused before
  `deleteTopics`. `ifEmpty` refuses when the estimate is above zero. The etag is the topic's id and its
  partition count, so a recreate or an increase refuses the delete and a commit on a group does not.
- A partition increase is previewed with `createPartitions` `validateOnly` and applied without it. A shrink,
  and any replica assignment, are refused before that call. The preview says records already written stay on
  their partitions and a key can land on a new partition afterwards.
- Deleting a group never removes its members. `disconnectConsumers` is ignored. A group with members, or one
  that is rebalancing, fails `FAILED_PRECONDITION` and names the members. A streams group stays read-only
  with the existing reason. A share group is deleted only where share groups are declared, and only when it
  is empty.

## Consequences

- A caller can show the effect of a topic or group change before anything is changed, and a stale etag
  changes nothing.
- `disconnectConsumers` no longer empties a Kafka group. Callers that set it to force a delete now get a
  refusal, and the members stay. That is a breaking change of behaviour for the Kafka driver. The flag
  remains on `Operation.Delete` for systems that still honour it.
- Group membership of a topic is not part of the topic etag. A caller that must refuse when a new group
  appears reads the groups again itself.

## Alternatives considered

- **Create the topic inside the preview and delete it on cancel.** A preview that is abandoned leaves the
  topic behind. `validateOnly` does not.
- **Put every group id into the topic etag.** A commit on a busy topic would make every delete stale.
- **Honour `disconnectConsumers` for group delete.** It removes members the caller did not name as the thing
  being deleted.
