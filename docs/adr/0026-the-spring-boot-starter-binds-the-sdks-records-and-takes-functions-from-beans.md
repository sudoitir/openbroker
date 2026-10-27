# ADR-0026: The Spring Boot starter binds the SDK's records and takes functions from beans

- **Status**: accepted
- **Date**: 2026-10-27
- **Deciders**: Mahdi Amirabdollahi

## Context

Most JVM services that would use OpenBroker are Spring Boot applications, and each wrote the same glue: building
`ConnectRequest`s from its own properties, wiring TLS, tracing and health, and closing clients in order. The SDK's
options are immutable records with defaults and validation, so they can be bound from configuration. But Spring's
constructor binding passes `null` for every component the configuration leaves out, and the records reject `null`.
`ConnectOptions` also carries functions (interceptor, TLS, clock, context propagation, observer) that no property can
express. Some applications declare connections in configuration; others open them at runtime from their own store.

## Decision

`openbroker-spring-boot-autoconfigure` binds each connection's options to the SDK's own records, component by
component, through Spring's `Binder`. A component that is not set takes the record's own default (`defaults()`), so a
partial block works and no option is mirrored. Interface-typed components, such as the dead-letter target, are set in
code; setting them in configuration fails the start. Credentials bind through one properties type that names the form.
Function-valued options come from beans: SSL bundles for TLS, a `CredentialSource` qualified by the connection's name,
a `CallInterceptor`, a `Clock`, Micrometer context propagation, and ordered `ConnectOptionsCustomizer`s. The same
pieces (`BrokerDrivers`, `OpenBrokerConnectOptions`, `SslBundleTls`) are beans for connections opened at runtime;
health and lifecycle apply only to the connections the starter creates.

## Consequences

- Adding a component to an options record makes it configurable with no starter change.
- The configuration processor cannot see through the binder, so the starter keeps a metadata file, and a test checks
  that it lists exactly the bindable keys.
- An application that manages its own connections still gets the same observers, TLS and defaults, and decides itself
  whether a managed target is part of its health.

## Alternatives considered

- Bind the records directly: a partial block fails on `null`.
- A parallel set of JavaBean properties classes: every option doubled, and they drift.
- `@DefaultValue` on the SDK records: Spring in the API, which ADR-0027 forbids.
- One registry bean for every connection: injection by a string name, and lifecycle hidden.
