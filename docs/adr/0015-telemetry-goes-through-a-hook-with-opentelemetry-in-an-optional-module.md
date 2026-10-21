# ADR-0015: Telemetry goes through a hook, with OpenTelemetry in an optional module

- **Status**: accepted
- **Date**: 2026-10-21
- **Deciders**: Mahdi Amirabdollahi

## Context

Operators want traces and metrics for send, receive, process and settle, with context carried from
sender to handler. `openbroker-api` depends on JSpecify only (CLAUDE.md, non-negotiable 1). The OTel
messaging semantic conventions are still in development.

## Decision

- `MessagingObserver` is in the API, set with `ConnectOptions.withObserver`. The default is
  `MessagingObserver.none()`, which allocates nothing.
- The driver, or the engine in `openbroker-support`, calls `onSend`, `onReceive`, `onProcess` and
  `onSettle` with an `OperationInfo`. On send the observer injects carrier entries, which become
  string message properties (`traceparent`, `tracestate`).
- `openbroker-opentelemetry` depends on `openbroker-api` and `io.opentelemetry:opentelemetry-api`
  only. `OpenTelemetryMessaging.of(OpenTelemetry)` maps the hook to spans and metrics and defines the
  attribute names itself, because the conventions are not stable.

## Consequences

- Adopters without OTel pay nothing and add no dependency.
- The attribute names may change with the conventions; the module is versioned with them.
- Context rides in message properties, visible to every consumer of the message.

## Alternatives considered

- **A Micrometer Observation API.** A second facade.
- **The OTel API in `openbroker-api`.** Breaks the one-dependency rule.
- **Java-agent instrumentation only.** It does not know OpenBroker's links.
