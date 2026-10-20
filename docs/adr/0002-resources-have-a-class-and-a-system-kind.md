# ADR-0002: Resources have a class and a system's kind

- **Status**: accepted
- **Date**: 2026-10-20
- **Deciders**: Mahdi Amirabdollahi

## Context

A tool needs to say "every queue-like thing" without knowing whether it is an Artemis queue, a Kafka
consumer group's partition set or an SQS queue, and still keep each system's own precision.

## Decision

Every resource has a `ResourceClass` and a `KindId`.

- `ResourceClass` is closed: `NAMESPACE, DESTINATION, PARTITION, SUBSCRIPTION, CONSUMER_GROUP, ROUTE,
  POLICY, ACCESS_RULE, PRINCIPAL, CONNECTION, CHANNEL, PRODUCER, CONSUMER, TRANSACTION`. "Role" was
  rejected because it clashes with security roles; "session" became `CHANNEL` because a Service Bus
  session is an ordered group and a Solace session a connection.
- A `KindId` is `<system>.<name>` (`artemis.queue`), declared by the driver with its class, verbs and
  attribute schema.
- A `SUBSCRIPTION`'s destination is optional: a standalone queue has none and is sent to with
  `SendTarget.subscription`. Links between resources are `ROUTE`s, whose target is local or remote.
- A `ResourceId` is `kind/segment/…` with strict percent-encoding: parsing refuses a non-canonical
  form, so equal text is an equal id.
- Standard attributes and metric keys (`openbroker.*`) carry what every system has; native attributes
  are namespaced by system.

## Consequences

- Queries by class work across systems; queries by kind keep a system's exact meaning.
- The class set is a consumer's exhaustive `switch`, so it must be complete before 1.0.

## Alternatives considered

- **Per-system types only.** No cross-system query.
- **One generic "queue" and "topic".** Loses Kafka partitions, consumer groups and RabbitMQ exchanges.
