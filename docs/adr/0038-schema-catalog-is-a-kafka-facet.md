# ADR-0038: The schema catalog is a Kafka facet

- **Status**: accepted
- **Date**: 2026-11-26
- **Deciders**: Mahdi Amirabdollahi

## Context

A Kafka cluster is often managed together with a schema registry: subjects, versions, and a compatibility level. A record may carry a schema id in front of its payload. That is native Kafka behaviour. Artemis, RabbitMQ and the other systems this SDK speaks have no subject and no schema id. Putting subjects on `ResourceClass` or `Operation` would make every driver pretend to have them.

The preview vocabulary already exists: `Preview`, `Precondition.ifMatch`, and `EtagBasis`. A registry does not expose an incarnation id for a subject, so an etag over the subject's name, versions and compatibility is configuration, not incarnation.

## Decision

- The catalog is `SchemaCatalog`, reached by `ClusterConnection.unwrap(SchemaCatalog.class)`. It is not a neutral facet and not a new `ResourceClass` or `Operation`.
- The capability id is `kafka.schema-registry`. With no registry configured it is unavailable, and the driver invents no subject. Unknown is not unavailable: a named registry that has not been reached stays unknown.
- A registration, a compatibility change and a delete are previewed with `Preview`, applied with `Precondition.ifMatch`, and etagged with `EtagBasis.CONFIGURATION`. The enforcement is `CHECKED_BEFORE`, because the registry has no atomic compare-and-set. A mismatch is `FAILED_PRECONDITION` / `PRECONDITION_FAILED` and writes nothing.
- This capability is proved by a Kafka driver test against a real registry. It is not a TCK facet. A TCK facet is a contract every driver must pass, and a driver with no registry has nothing to pass.

## Consequences

- A caller that wants subjects unwraps. A caller that only uses the neutral facets never sees them.
- The etag does not tell a subject deleted and created again with the same versions and compatibility apart from the one that was previewed. That is what `CONFIGURATION` means.
- A change between the check and the write is not detected. The preview says so.

## Alternatives considered

- **A neutral `Schema` facet on every driver.** Rejected. It would force systems with no registry to return empty lists and pretend that was an answer.
- **A TCK facet gated on the capability.** Rejected. The TCK would need a registry fixture for one driver and a skip for every other. A driver test against a real registry is the proof that matters.
