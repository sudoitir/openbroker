# ADR-0018: The Kafka mapping and its honest destructive operations

- **Status**: accepted; classic-group point superseded by [ADR-0031](0031-kafka-3-0-and-later-declared-per-feature.md)
- **Date**: 2026-10-27
- **Deciders**: Mahdi Amirabdollahi

## Context

Kafka is a log, not a queue. A topic is split in partitions, a consumer group keeps one committed
offset per partition, nothing is acknowledged one message at a time, and records leave only from the
start of the log. The SDK's model was drawn for brokers of queues, and the Kafka driver must map onto
it without pretending Kafka is what it is not. Four things need a decision: what each Kafka thing is
in the model, what a "backlog" and a "purge" mean, what can honestly be offered per message, and how a
driver that must read the cluster to learn its version and features says what it found.

## Decision

**The mapping.**

| Kind | Class | What it is |
|---|---|---|
| `kafka.topic` | DESTINATION | a topic; its settings are config attributes (`kafka.config.<name>`) |
| `kafka.partition` | PARTITION | a partition, placed on its replicas (leader and followers, in sync or not) |
| `kafka.group` | CONSUMER_GROUP | a group of any type, with its state, members and generation |
| `kafka.group-topic` | SUBSCRIPTION | a consumer group on a topic |
| `kafka.share-group-topic` | SUBSCRIPTION | a share group on a topic; declared only where share groups run in production |
| `kafka.acl` | ACCESS_RULE | an ACL, identified by what it says |
| `kafka.transactional-id` | TRANSACTION | a transactional id and the transaction it may have left open |

Brokers are nodes. The controller quorum marks the nodes that vote, adds the controllers that are not
brokers, and says whether each voter keeps up. A broker is under-replicated when a partition lists it as
a replica outside the in-sync set; that is read from one description of the topics, bounded, kept for the
watch interval and shared by node health, the partition kind and the watch, and node health says as of
when.

**Who owns a group's offsets.** A consumer-protocol group, a share group and a group made by
committing offsets alone are subscriptions the driver may change, because the cluster files the last
as a classic group with no protocol and no application owns it. A classic group an application
runs, and a streams group, are listed read-only with the reason that their offsets belong to the
application, and offer no subscription.

**A backlog is a lag, and a lag is an estimate.** It is the end offset less where the group stands,
summed, reported as an estimate with each partition's share. A partition the group has no offset on is
left out of the total and named with its reason, never counted as zero. Transaction markers and
compaction make offset arithmetic overcount, and the end offset moves while it is read; the estimate's
condition says so. A share group's lag is the one the cluster reports, else the end less its start offset.

**Destructive operations name what they do.** Purge and seek move one group's offsets and no other's, and
only while the group has no member: the preview lists the members, and a run against a group that has
some fails `FAILED_PRECONDITION` and changes nothing, rather than removing members as a side effect. The
etag fingerprints the group's generation, state and offsets, so a run after any change is refused.
Truncate deletes from the start of the log for every group, so its preview names each group, of any type,
that loses records it has not read, and how many; the etag fingerprints the partitions' log starts. A
topic that only compacts is refused in the preview, naming its cleanup policy, before Kafka refuses it.

**Browsing reads, and joins nothing.** A browse is an assigned consumer with no group id and no commit,
so no group appears and none moves. It reads what a read-committed consumer reads, because a
subscription's isolation level is a client setting no Admin call can see. A page is bounded by the count,
the bytes of body kept per message and a cap on the page.

**Per-message actions are unavailable, with the reason.** The log is append-only. Deleting one record is
impossible, and moving or copying one is writing it again at a new offset, which would lie about the log.
They are declared unavailable as capabilities and refused as bulk actions, and a filter is refused for
browsing and counting, because Kafka cannot filter where it keeps the records.

**The cluster's version and features are read from the software, not guessed.** A broker does not report
its release, so the driver reads the newest `metadata.version` its software supports: 3.9 stops at level
21 and 4.0 begins at 22, which is how a cluster before 4.0 is refused. Share groups were a preview in
4.1 behind the same `share.version` feature that production clusters finalize in 4.2, so the share kind is
declared only where the software supports `metadata.version` 28 (4.2-IV0) or more and `share.version` is
finalized at 1 or more.

**A message is a record.** The standard fields travel as headers under `openbroker.`, a property is a
header of its own name, and a property that is not a string names its type in a companion header. A key,
a group id or a routing key becomes the record key. A priority, an expiry or a delivery time is refused:
a record has nowhere to keep it.

**The watch polls.** Kafka sends no events, so the watch lists the cluster every `watch.interval`
(`PT10S`) and reports creations and deletions of topics, partitions, groups and subscriptions, all on
the cluster as one stream.

**A change that returns has taken effect.** Each broker serves metadata from its own copy, which trails
the controller, so a list that follows a write could show the old state. A write waits until several
reads agree before it returns; giving up at the deadline is not a failure, because the write was made.

## Consequences

- An operator sees the same things on Kafka as on any broker: destinations, subscriptions with a
  backlog, destructive operations with a preview and an etag.
- The Kafka-specific honesty is visible, not hidden: lags say they are estimates and which partitions are
  unknown, a group with members cannot be purged, and truncation lists its victims.
- A purge or seek of a group that is running needs the operator to stop it first. That is the price of not
  changing a running application as a side effect.
- Writes cost a few hundred milliseconds more, spent confirming they show.
- The watch is as late as its interval and reports only creations and deletions; a member joining a group
  is not an event.
- A classic application group cannot be reset through the driver, though Kafka allows it when the group
  is empty; the decision is that the application owns it. A group an operator makes by committing offsets
  is the exception because nothing else owns it.
- Reading under-replication from the shared description is cheap and may be ten seconds old; node health
  says when it was read.

## Alternatives considered

- **Remove the members of a group to purge it.** Changes running applications as a side effect; a
  separate, explicit operation if ever wanted.
- **Treat every classic group as manageable.** Simple and what Kafka's own tool does, but the offsets of
  a running application's group are the application's to move.
- **Report a lag as exact.** It is not: markers, compaction and a moving end offset all bend it.
- **Delete or move a single record by copying and skipping.** It would rewrite offsets and lie about the
  log.
- **Report a partition with no offset as zero lag.** It would read as caught up when it is unknown.
- **Take the release from the broker's reported version.** No Admin call reports it; the supported
  feature range is the nearest honest signal.
- **Listing every topic on each node-health read.** A large cluster pays for it on every call; one shared,
  bounded description pays once an interval.
