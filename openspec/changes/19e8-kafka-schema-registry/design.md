## Context

See ADR-0038, ADR-0039 and ADR-0040. The catalog is a Kafka facet reached by unwrap. A serde is a registered name. One vocabulary is shared where it holds. The driver does not model a registry it cannot speak to.

## Goals / Non-Goals

**Goals:**

- List subjects, versions and compatibility on a Confluent-compatible registry.
- Preview a registration, a compatibility change and a delete, writing nothing, and refuse a stale etag.
- Decode and encode Avro, Protobuf and JSON so a record round-trips.

**Non-Goals:**

- A Glue client. Its schema ids are UUIDs, and it needs an SDK this driver does not carry. Until a later change, `registry.kind` does not offer it. See ADR-0040.
- A custom serde loaded by class name.
- Protobuf schemas that import another file.

## Decisions

The three ADRs are the decisions. This change follows them.

## Risks / Trade-offs

- The etag is configuration, not incarnation, and the check is not atomic. A change between the read and the write is not detected. The preview says so.
- [Risk] A registry that treats the schema lookup as a write would make a preview write. The driver test asserts the version list is unchanged after a preview.

## Migration Plan

- No migration. Callers that do not unwrap see no change.

## Open Questions

- None.
