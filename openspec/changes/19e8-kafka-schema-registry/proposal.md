## Why

A Kafka cluster is often managed with a schema registry, and a record may carry a schema id. The driver has no way to list subjects, preview a registration, or decode that record. The other drivers have no registry, so this stays a Kafka facet.

## What Changes

- A connection unwraps to a schema catalog: subjects, versions, compatibility, and a preview before a registration, a compatibility change or a delete.
- A payload that carries a schema id decodes for Avro, Protobuf and JSON. Encoding the same record produces framing that decodes back to it. A serde is a registered name, never a class name.
- With no registry configured the capability is unavailable and no subject is invented. A registry kind other than none or confluent is refused. The driver does not model a registry it cannot speak to.
- No API break. The new types are on the Kafka driver. `openbroker-api` gains no dependency.

## Capabilities

### New Capabilities

- None.

### Modified Capabilities

- `broker-sdk-kafka`: A schema catalog on a Kafka connection, previewed changes, and decoding a record that carries a schema id.
