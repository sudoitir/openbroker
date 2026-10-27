# ADR-0025: The chaos fixture names the serving node and its successor

- **Status**: accepted
- **Date**: 2026-10-27
- **Deciders**: Mahdi Amirabdollahi

## Context

[ADR-0017](0017-chaos-is-part-of-the-tck.md) tests failover with a shared-store primary and backup
pair, and `TckChaos` exposed `primary()` and `backup()`. That shape is Artemis's. A partitioned log
fails over per partition: the node that serves a subscription is its partition's leader, and the node
that takes over is an in-sync follower. Which node that is depends on the subscription and can change.

## Decision

- `TckChaos` has `serving(ResourceId subscription)` and `successor(ResourceId subscription)` in place
  of `primary()` and `backup()`. A fixture resolves them when they are called.
  - Artemis maps them to its pair's live member and its backup.
  - A partitioned system maps them to a partition's leader and an in-sync follower.
- `node()`, `haRequest()` and the cut, heal, kill and start operations stay.
- `ChaosTck` opens its processors with `Scope.whole()` rather than a scope on the primary, and a
  driver reads the serving member of a group it is scoped to as a whole.
- A case that disturbs nodes heals every node it touched when it ends.
- A message stored twice after a kill and restart must have been reported with `resent=true`.
- Deduplication assumptions ask for a minimum scope ([ADR-0020](0020-deduplication-is-declared-as-a-scope.md)).

## Consequences

- A driver whose failover is not a fixed pair can run the same failover cases.
- The cases name the serving node before they disturb it, because after a kill a leader cannot be asked.
- This is a breaking change to the TCK's fixture, which `0.x` allows.

## Alternatives considered

- **Keep `primary()` and `backup()` and let a partitioned driver pick fixed nodes.** The chosen nodes
  would not be the ones serving the subscription, so the cases would test nothing.
- **One method returning the whole replica set.** The cases need to say which node to kill and which
  should take over.
