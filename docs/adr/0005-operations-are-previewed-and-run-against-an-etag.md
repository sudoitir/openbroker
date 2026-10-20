# ADR-0005: Operations are previewed, and run against an etag

- **Status**: accepted
- **Date**: 2026-10-20
- **Deciders**: Mahdi Amirabdollahi

## Context

Destructive operations on brokers (purge, delete, move) are often approved by one person and run
later. Between the two, a queue can be deleted and recreated, or a node can join; running the approved
operation then acts on something nobody approved.

## Decision

- `Operation` is sealed: `Create`, `Update` (a merge patch), `Delete(ifEmpty, disconnectConsumers)`,
  `Pause`/`Resume(Direction)`, `Purge`, `Seek(Position)`, `Truncate(Position)`, `Close`, `Reassign`,
  `ElectLeader`, `Resolve`.
- `preview(resource, operation, scope)` returns the diff, whether it replaces the resource, an estimate
  and an etag (AIP-154) over the resolved shards and each shard's incarnation, plus how the
  precondition is enforced: `ATOMIC` or `CHECKED_BEFORE`.
- `execute(resource, operation, scope, Precondition)` takes `Precondition.none()` or
  `Precondition.ifMatch(etag)`. The precondition is part of the request, as in Kubernetes, not a field
  of the operation. A mismatch is `FAILED_PRECONDITION`.
- Volume changes never change the etag: the estimate is for the approver.

## Consequences

- An approval can pin exactly what was approved, across nodes.
- A system without server-side preconditions checks before acting; the window is documented as
  `CHECKED_BEFORE`.

## Alternatives considered

- **Compare counts before running.** Refuses harmless growth and accepts a recreated resource.
- **A version field in each operation.** Mixes intent with concurrency control.
