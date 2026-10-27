# ADR-0029: Micrometer owns metrics, OpenTelemetry owns traces

- **Status**: accepted
- **Date**: 2026-10-27
- **Deciders**: Mahdi Amirabdollahi

## Context

ADR-0015 put telemetry behind the `MessagingObserver` hook, with OpenTelemetry in an optional module. A connection had
one observer. Spring Boot applications export metrics through Micrometer, often over OTLP, and traces through
OpenTelemetry; the OpenTelemetry observer records both spans and metrics, so wiring it next to a Micrometer observer
would count every message twice. Some tools relay messages that producers wrote, and the observer overwrote the
producer's trace context.

## Decision

The API gains `MessagingObserver.composite`, so one connection feeds several observers. The Spring Boot starter
records client metrics through a Micrometer observer when a `MeterRegistry` exists, using the OpenTelemetry instrument
names, and then hands the OpenTelemetry observer a view whose meter provider is no-op, so it emits spans only. Without
Micrometer, the OpenTelemetry observer emits both. `OpenTelemetryMessaging.builder(...).injectContext(false)` traces
sends without writing into the message.

## Consequences

- Metrics appear once, under the same names whichever exporter carries them.
- A composite observer adds one indirection per observation; it allocates nothing when none is configured.
- Tools that relay messages keep the producer's trace intact.

## Alternatives considered

- Both observers record metrics: double counting.
- OpenTelemetry for metrics too: against Spring Boot's convention, and invisible to Micrometer-only applications.
- A tracing-only flag on the OpenTelemetry observer instead of a meter-provider view: a second switch for what the
  OpenTelemetry API already expresses.
