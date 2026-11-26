# ADR-0040: Glue shares the catalog

- **Status**: accepted
- **Date**: 2026-11-26
- **Deciders**: Mahdi Amirabdollahi

## Context

A Confluent-compatible registry is not the only registry a Kafka cluster is managed through. AWS Glue Schema Registry speaks the same words: subject, version, schema type, compatibility. It does not speak every operation, and it does not identify a schema version the way a record's schema id does. A Glue schema version id is a UUID. `SubjectVersion.id` is the integer a record carries in front of its payload.

Glue's client would also need credentials. The SDK's default provider chain reads the ambient environment. A connection that did not set a secret would then act as whoever the process happens to be. That is not a choice the caller made.

## Decision

- Glue is the same catalog, chosen by `registry.kind=glue`. Subject, version, schema type and compatibility mean the same thing. There is no second vocabulary and no Glue-only type.
- An operation Glue does not offer is `Assessment.unavailable` with Glue's reason, and the method throws a `BrokerException` naming that reason. The driver does not fake the operation, and it does not invent an integer id for a UUID.
- Until a caller can address a Glue version without that invention, the catalog reports the capability unavailable and refuses every operation with that reason. `software.amazon.awssdk:glue` is not a dependency of this change.
- Credentials, when Glue is reached, are the connection's `registry.credentials` or an explicit choice to use the ambient chain. There is no silent ambient default. A missing region, registry name or credential is named in the reason.

## Consequences

- A connection with `registry.kind=glue` does not list subjects today. The reason says why. The Confluent-compatible path is the one that is complete.
- Adding the Glue client later does not change the catalog. It fills the operations Glue actually has and keeps refusing the ones it lacks, including a decode that would pretend a UUID was the integer in front of a record.
- A deployment that expected the process role to be picked up without saying so will find the capability unavailable. That is the point.

## Alternatives considered

- **Map Glue's version number onto `SubjectVersion.id`.** Rejected. The id is the number a record carries. A Glue record does not carry that number. Returning the version number would be a fake id.
- **Use the SDK's default credential chain when `registry.credentials` is unset.** Rejected. An unset secret must not become whoever the process is.
