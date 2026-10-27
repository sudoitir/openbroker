# ADR-0028: Connect validates options against the driver's schema

- **Status**: accepted
- **Date**: 2026-10-27
- **Deciders**: Mahdi Amirabdollahi

## Context

Each driver describes its cluster options in a `ConnectionSchema` (key, type, required, sensitive, allowed values).
Nothing checked a `ClusterSpec`'s options against it, so a misspelt key such as `sasl.mechansim` was silently ignored
and the connection ran with the default, often failing later with an unrelated authentication error.

## Decision

`ConnectionSchema.validate` reports every unknown key, missing required key, wrong type and value outside the allowed
values. `BrokerDrivers.connect` and `verify` refuse a cluster with violations, before any call is sent, with
`INVALID_ARGUMENT`, `NOT_SENT`, and a message that names each key and never a sensitive value.

## Consequences

- Every consumer, with or without a framework, fails fast on a wrong option.
- Breaking: a caller that passed options its driver does not declare must remove them. Released as a minor version
  while `0.x`.
- Drivers must declare every option they read; the Kafka and Artemis drivers already do.

## Alternatives considered

- Validate only in the Spring Boot starter: other consumers keep the silent failure.
- Warn instead of refuse: a warning in a log is how the misspelling went unnoticed.
