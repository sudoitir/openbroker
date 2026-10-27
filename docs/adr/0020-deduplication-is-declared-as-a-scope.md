# ADR-0020: Deduplication is declared as a scope

- **Status**: accepted
- **Date**: 2026-10-27
- **Deciders**: Mahdi Amirabdollahi

## Context

[ADR-0010](0010-the-transport-leaves-incubation-with-declared-guarantees.md) declared deduplication
as a boolean, and [ADR-0012](0012-sending-is-bounded-and-backpressure-is-a-value.md) made ordered
at-least-once sending depend on it, because a resend only keeps order when the system drops the
sender's own repeats. A boolean cannot say what systems really offer:

- Kafka's idempotent producer drops its own retries while its producer session lasts, and has no
  broker-side memory of caller-set ids.
- Artemis, SQS FIFO, JetStream and Azure Service Bus drop any repeat of a caller-set id within a window.
- Pulsar keeps a sequence per named producer, and ActiveMQ Classic a producer audit.
- RabbitMQ, Pub/Sub and Solace drop nothing.

Declaring Kafka's behaviour unavailable would refuse ordered at-least-once sending on the system where
ordering matters most. Declaring it available would promise caller-id deduplication it cannot keep.

## Decision

- `DeduplicationScope` is `NONE`, `SESSION` or `WINDOW`.
  - `SESSION`: the system drops the sender's own re-sends while its producer session lasts.
  - `WINDOW`: the system drops any repeat of a caller-set deduplication id within a window.
- `TransportCapabilities.deduplication` is the strongest scope the transport offers.
- `SenderOptions.deduplication` replaces `idempotent`: the minimum scope the sender needs, `NONE` by
  default. An ask above the declared scope fails `UNIMPLEMENTED`, request not sent.
- Ordered at-least-once sending needs at least `SESSION`. The pipeline stamps a deduplication id on
  every message of a sender that asks for any scope above `NONE`.
- `Sender.deduplication()` still reports the window, and only under `WINDOW`.
- The scopes are not strictly nested: a `WINDOW` system meets a `SESSION` ask only for re-sends that
  arrive within its window. The javadoc says so.
- Artemis declares `WINDOW`, with its duplicate-id cache as the window.

## Consequences

- Kafka can declare what it does, and ordered sending works on it.
- The TCK asks each case for the scope it needs: ordering and the kill-and-restart cases need
  `SESSION`, and the cases that repeat a caller-set id need `WINDOW`.
- Adopters of `0.2.0` change `withIdempotent(true)` to `withDeduplication(DeduplicationScope.SESSION)`.
  This is a breaking change, which `0.x` allows with a minor version.

## Alternatives considered

- **Keep the boolean and declare Kafka without it.** Refuses ordered at-least-once sending on Kafka.
- **Keep the boolean and declare Kafka with it.** Promises caller-id deduplication Kafka cannot keep.
- **Two booleans, one for resends and one for ids.** Two flags for a ladder; a scope reads as one.
