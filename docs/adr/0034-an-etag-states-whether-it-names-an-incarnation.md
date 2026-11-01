# ADR-0034: An etag states whether it names an incarnation

- **Status**: accepted
- **Date**: 2026-11-01
- **Deciders**: Mahdi Amirabdollahi

## Context

A `Preview`'s etag promises that an operation on a resource recreated since the preview is refused. Artemis
queues and Kafka topics carry a system-assigned id, so the promise holds. RabbitMQ exposes none: its management
API names a queue by virtual host and name, and a queue deleted and declared again with the same arguments is
indistinguishable. A driver that keeps the promise by guessing would lie; one that silently cannot keep it would
make the TCK fail for a limit of the system.

## Decision

`Preview` carries an `EtagBasis`:

- `INCARNATION`: the etag holds a system-assigned identity, so a recreated resource is refused.
- `CONFIGURATION`: the system exposes no such identity; the etag holds the configuration and the member set, so a
  change of either is refused but a resource recreated with the same configuration is not told apart.

Each driver declares the basis per operation from what its etag actually holds. The TCK keeps the grown-backlog
half of its etag case for every driver and checks the recreate refusal only for `INCARNATION`, skipping it with
the reason otherwise.

## Consequences

- A caller can tell an operator exactly how strong the guarantee is.
- `Preview` gains a record component: a breaking change, taken in the 0.9 minor release before 1.0.

## Alternatives considered

- **Synthesise an incarnation** from creation time or message statistics: not stable, and wrong after a restart.
- **Drop the promise for every driver**: weakens Artemis and Kafka for a limit of one system.
