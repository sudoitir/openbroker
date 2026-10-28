# ADR-0031: Kafka 3.0 and later, declared per feature

- **Status**: accepted
- **Date**: 2026-10-28
- **Deciders**: Mahdi Amirabdollahi

## Context

The Kafka driver supported Kafka 4.0 and later. Most running estates are on Kafka 3.x, many in
ZooKeeper mode, and the 4.x clients speak to them fully. Two rules of ADR-0018 kept those estates out
or half served: the driver refused them, and it treated every classic group an application runs as
read-only. Kafka itself allows an offset reset on a classic group that is empty, and the classic
protocol is still the default of most clients, so most real groups were unmanageable. Separately,
`verify` read the cluster through whichever broker answered, so a broker advertising an address the
caller cannot reach passed the check and failed later.

## Decision

**The range is Kafka 3.0 and later, in ZooKeeper and KRaft mode.** The driver reads the cluster's
features once per connection: the supported `metadata.version` maximum, the finalized levels of
`group.version`, `streams.version`, `eligible.leader.replicas.version` and `transaction.version`, and
whether a KRaft quorum can be described. The release comes from the `metadata.version` table; without
that feature, a cluster that answers `listTransactions` (KIP-664) is "at least 3.0" and one that does not
is refused.

**What a cluster lacks is declared per feature, with its reason and, where an operator can change it, the
remedy. It is never emulated.** The quorum, consumer-protocol groups, share groups, streams groups, fenced
brokers and eligible leader replicas each become an unavailable capability. The messaging client joins
with the consumer protocol where the cluster has it and with Kafka's classic protocol where it does not.

**Empty, idle classic groups are subscriptions.** This supersedes the classic-group point of ADR-0018.
Consumer, share and classic groups are manageable; streams groups, and classic groups whose assignor is
Kafka Streams', stay read-only because the streams application owns their offsets and state. Purge and
seek refuse a group that has members, as before, and now also one that is rebalancing, since its
membership is about to change.

**Verify reaches every broker on the address it advertises.** Each broker is resolved, connected to
and asked for its configuration, in parallel and within the caller's deadline, and an unreached broker
is a failed node with the cause (it does not resolve, refused, timed out, TLS, authentication, no
answer) while the others keep their descriptions.

## Consequences

- Kafka 3.x estates connect, with the gaps stated instead of hidden.
- A classic group of a running application can be rewound only while it is empty; a tool that resets
  offsets must expect `FAILED_PRECONDITION` while the application runs.
- Early KRaft releases (3.0 to 3.2) are labelled ZooKeeper mode; they were never production-ready.
- A ZooKeeper cluster before 3.3 with a broker down fails verify as a whole, because `listTransactions`
  fans out to every broker.
- The test matrix grows by a ZooKeeper-mode leg, which costs about 2.5 GB of memory.
- ADR-0018 is not edited beyond its status line.

## Alternatives considered

- **Keep 4.0 as the floor.** It leaves most estates unserved for a limit of ours, not the cluster's.
- **Read versions from ApiVersions.** The public `Admin` does not expose them; only internal classes
  do, and those break on upgrades.
- **Remove a group's members to reset it.** It changes a running application as a side effect.
- **Emulate consumer-protocol or share groups on older clusters.** It would promise semantics the
  broker does not enforce.
- **Verify through one broker.** It is the bug: a healthy answer from one says nothing of the others.
