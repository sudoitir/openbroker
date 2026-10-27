# ADR-0022: Share groups are a subscription kind

- **Status**: accepted
- **Date**: 2026-10-27
- **Deciders**: Mahdi Amirabdollahi

## Context

Kafka has two ways to consume a topic. A consumer group gives each member whole partitions, in
order, and settles by offset ([ADR-0019](0019-kafka-settles-by-offset-and-runs-keys-in-lanes.md)). A
share group lets every member read every partition, acknowledges each record on its own, counts the
deliveries and gives a record up after a limit. They differ on the broker in ordering, settlement,
transactions and retry, so one set of capabilities for "Kafka" would be untrue for one of them.
Share groups are production-ready from Kafka 4.2; the broker dead-letters them from `share.version` 2
(Kafka 4.4).

## Decision

- **A share group is its own subscription kind**, `kafka.share-group-topic`, beside `kafka.group-topic`.
  A processor on it uses a share consumer in explicit acknowledgement mode: accept, release and reject
  map to `ACCEPT`, `RELEASE` and `REJECT`.
- **Its capabilities are its own**: no ordering but `NONE`, settlement per message, no joined
  transactions, and `SystemRetry(false, true, deadLetters)`: the broker counts attempts, does not delay,
  and dead-letters only at `share.version` 2. Only `AT_LEAST_ONCE` is offered. A pull receiver is not
  offered yet.
- **The kind is declared from the cluster's features** when capabilities are first asked for: the
  software supports the metadata level of Kafka 4.2 (28) and `share.version` is finalized at 1 or more.
  A preview cluster (4.1) is not declared. The features are read again at every open, and an open on a
  cluster that no longer offers the kind fails `UNIMPLEMENTED` naming the reason and the remedy.
- **Retry follows the broker.** The default resolves to the system policy: a failed record is released
  counted, and the broker's `share.delivery.count.limit` decides when it is given up. A managed policy
  that allows more attempts than that limit is refused at open, because it could never reach its last
  attempt.
- **Dead-lettering.** Where `share.version` is 2, a rejected record, or one past the limit, goes to the
  group's `errors.deadletterqueue.topic.name`. A system policy on a group that names none is refused at
  open with a remedy naming the group config, the cluster's topic-name prefix and the topic's
  `errors.deadletterqueue.group.enable`. Elsewhere a rejection is a managed dead letter: the record is
  sent to `<topic>.dlt` (or the policy's target), which is created when the processor opens, and then
  rejected.
- **Long handlers renew their lock.** The poll thread acknowledges every record it holds with `RENEW`
  at half the acquisition lock time, and recognises the renewed record when it comes back. The consumer
  is polled only when a record finished, a renewal is due, or there is credit to fill, because a
  renewed record is returned at once and would otherwise spin the loop. It has no API.
- **A release with a delay** keeps the lock renewed until the delay has passed, then releases the
  record: the broker has no delay of its own.
- A new group reads from the start of its topic, as a consumer group of this driver does: the group's
  `share.auto.offset.reset` is set to `earliest` at open unless someone set it.

## Consequences

- A share group's records are not ordered, and a slow handler keeps a record from every other member.
- The exact release and feature level are tied to Kafka's: when the broker adds levels, the declaration
  follows by reading them.
- At `share.version` 1 the default policy ends in the broker archiving a record past the limit, with
  no copy: the managed policy is the way to keep it.
- A renewal is a request per held record every half lock time, plus one per poll while credit is free.

## Alternatives considered

- **A flag on the processor.** The two kinds differ on the broker in ways the flag would hide, and the
  capabilities are per kind.
- **Emulate dead-lettering in the client for the system policy.** It would hide where the broker
  enforces ([ADR-0021](0021-the-retry-default-follows-what-the-system-enforces.md)).
- **Declare the preview.** Its behaviour was still changing between releases.
