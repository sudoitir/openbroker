# ADR-0040: Glue shares the catalog

- **Status**: accepted
- **Date**: 2026-11-26
- **Deciders**: Mahdi Amirabdollahi

## Context

A Confluent-compatible registry is not the only registry a Kafka cluster is managed through. AWS Glue Schema Registry uses the same words where they hold: subject, version, schema type, compatibility. It does not identify a schema version the way a record's schema id does. A Glue schema version id is a UUID. `SubjectVersion.id` is the integer a record carries in front of its payload. Speaking to Glue also needs an SDK this driver does not carry.

A client that answered anyway would have to invent that integer, or pretend an operation Glue does not offer had succeeded. This driver does not do either.

## Decision

- One vocabulary is shared where it holds. Subject, version, schema type and compatibility mean the same thing. There is no second vocabulary and no Glue-only type.
- The Glue client is a later change. Its schema ids are UUIDs, and it needs an SDK the driver does not carry. Until then `registry.kind` does not offer it. The driver refuses to model a registry it cannot speak to: a kind other than `none` or `confluent` is refused with the reason.
- An operation a registry does not offer is unavailable with that registry's reason, and the method throws a `BrokerException` naming the reason. The driver does not fake the operation, and it does not invent an integer id for a UUID.
- Credentials, when that later client is reached, are the connection secret or an explicit choice to use the ambient chain. There is no silent ambient default.

## Consequences

- A caller cannot select Glue today. The Confluent-compatible path is the one that is complete.
- Adding the client later does not change the catalog's vocabulary. It fills the operations Glue actually has and keeps refusing the ones it lacks.

## Alternatives considered

- **Ship a Glue class that reports every operation unavailable.** Rejected. It would ship in the jar and claim a capability the driver does not have.
- **Map Glue's version number onto `SubjectVersion.id`.** Rejected. The id is the number a record carries. A Glue record does not carry that number. Returning the version number would be a fake id.
- **Use an SDK default credential chain when no secret is set.** Rejected. An unset secret must not become whoever the process is.
