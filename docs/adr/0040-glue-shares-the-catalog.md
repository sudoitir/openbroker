# ADR-0040: Glue shares the catalog

- **Status**: accepted
- **Date**: 2026-11-26
- **Deciders**: Mahdi Amirabdollahi

## Context

A Confluent-compatible registry is not the only registry a Kafka cluster is managed through. AWS Glue Schema Registry speaks the same words: subject, version, schema type, compatibility. It does not speak every operation, and it does not identify a schema version the way a record's schema id does. A Glue schema version id is a UUID. `SubjectVersion.id` is the integer a record carries in front of its payload.

Glue's client would also need credentials. The SDK's default provider chain reads the ambient environment. A connection that did not set a secret would then act as whoever the process happens to be. That is not a choice the caller made.

## Decision

- Glue will share this catalog when a client can address a schema version without inventing an integer id. Subject, version, schema type and compatibility will mean the same thing. There is no second vocabulary.
- That client is a later change. `registry.kind` does not accept `glue`. The constant and the class remain, and they are not offered. `software.amazon.awssdk:glue` is not a dependency. The reason is the UUID schema version id: `SubjectVersion.id` is the integer a record carries, and a Glue record does not carry one.
- An operation the later client cannot offer will be `Assessment.unavailable` with Glue's reason, and the method will throw a `BrokerException` naming that reason. The driver will not fake the operation.
- Credentials, when that client is reached, are the connection's `registry.credentials` or an explicit choice to use the ambient chain. There is no silent ambient default.

## Consequences

- A caller cannot select Glue today. The Confluent-compatible path is the one that is complete. Adding the client later does not change the catalog's vocabulary.
- Adding the Glue client later does not change the catalog. It fills the operations Glue actually has and keeps refusing the ones it lacks, including a decode that would pretend a UUID was the integer in front of a record.
- A deployment that expected the process role to be picked up without saying so will find the capability unavailable. That is the point.

## Alternatives considered

- **Map Glue's version number onto `SubjectVersion.id`.** Rejected. The id is the number a record carries. A Glue record does not carry that number. Returning the version number would be a fake id.
- **Use the SDK's default credential chain when `registry.credentials` is unset.** Rejected. An unset secret must not become whoever the process is.
