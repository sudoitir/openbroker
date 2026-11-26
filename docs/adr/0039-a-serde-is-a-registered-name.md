# ADR-0039: A serde is a registered name

- **Status**: accepted
- **Date**: 2026-11-26
- **Deciders**: Mahdi Amirabdollahi

## Context

A payload that carries a schema id has to be decoded, and a caller producing a record wants bytes that decode back to that record. The three wire types a Kafka registry writes an id in front of are Avro, Protobuf and JSON. A caller, or a form field, can also name a serde. If that name were a Java class, `Class.forName` would load and run whatever the request named. That is remote code execution from a field.

The Confluent serializer artifacts are the usual implementation of this framing. They are not on Maven Central. The Protobuf and JSON Schema serializers are under the Confluent Community License, which a public Apache-2.0 SDK cannot depend on.

## Decision

- A serde is a name resolved against a map the installation registers. The map holds exactly `avro`, `protobuf` and `json`. Lookup is by name. There is no `Class.forName`, and no reflection on a value that came from a caller.
- An unknown name, including a Java class name, leaves the payload undecoded with `serde-not-registered`. Nothing is instantiated.
- A tombstone is `no-schema-id`, not an empty record. A payload with no framing is the same. An id the registry does not hold is `schema-not-found`. A serde that throws is `serde-refused`.
- The decoding stack, all Apache-2.0 and on Maven Central, is `org.apache.avro:avro` 1.11.5, `com.google.protobuf:protobuf-java` 3.25.8 (and `protobuf-java-util` at the same version if a transitive dependency pulls it), and `io.apicurio:apicurio-registry-protobuf-schema-utilities` 3.0.8. JSON is parsed with the Jackson already used by the other drivers (`tools.jackson`). Confluent's `kafka-*-serializer` artifacts are not used.

## Consequences

- A caller cannot plug in an arbitrary serde class through a request. An installation that needs another serde adds it to the map in code, under a name, and reviews that code.
- Protobuf schemas that import another file are refused. The converter is given one file. A schema with imports needs those files passed in, which this change does not do.
- A null field is omitted from the schema generated for an encode. A caller that must keep a null has no path yet.

## Alternatives considered

- **Depend on `kafka-avro-serializer` and its siblings.** Rejected. They are not on Maven Central, and the Protobuf and JSON Schema artifacts are not Apache-2.0.
- **`Class.forName` of a serde named on the topic.** Rejected. The name would come from a request or a form. Loading it is remote code execution.
