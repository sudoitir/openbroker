# Design: the Spring Boot starter

## Context

The SDK's options are immutable records with documented defaults, validated in their compact constructors, using durations, counts, byte sizes and enums. The "Options are plain values" requirement keeps them that way so they bind from configuration:
- `SenderOptions`, `ReceiverOptions` and `ProcessorOptions`;
- `RetryPolicy`, `FlowControl`, `SendBuffer`, `Backoff` and `Recovery`;
- `ClusterSpec`, with nodes, endpoints and a driver options map described by the driver's `OptionSpec` schema.

`ConnectOptions` mixes bindable values (timeouts, `maxResponseBytes`, spool directory) with function-valued ones (`CallInterceptor`, `TlsProvider`, `Clock`, context propagation, `MessagingObserver`). `openbroker-opentelemetry` provides an observer.

Spring Boot 4 facts, checked with ctx7:
- Auto-configurations are listed in `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`.
- Records bind by constructor without `@ConstructorBinding` when they have one constructor, with `@DefaultValue` for defaults.
- Health indicators implement `org.springframework.boot.health.contributor.HealthIndicator`.
- Actuator logs indicators slower than 10 s.

## Goals / Non-Goals

**Goals:**
- One `application.yml` block per connection, with the SDK's own records as the binding targets.
- Health, metrics and tracing that need no code.
- An orderly shutdown.

**Non-Goals:**
- A listener annotation model.
- Integration with Spring's own messaging abstractions.
- Spring Boot 3.

## Decisions

### D1. Bind the SDK's records, not a mirror
`OpenBrokerProperties` is a record holding `Map<String, Connection>`. `Connection` is a record of `system`, `ClusterSpec cluster`, `CredentialsProperties credentials`, `ConnectionOptions options`, `SenderOptions sender`, `ReceiverOptions receiver` and `ProcessorOptions processor`.
- **Rejected: a parallel set of JavaBean properties classes.** It doubles every option and drifts.

Where an SDK record will not bind (several constructors, a missing default), the record is fixed in `openbroker-api`, as its requirement asks. `ConnectionOptions` is the starter's own small record for the bindable part of `ConnectOptions` (timeouts, max response bytes, spool directory, SSL bundle name). The remaining parts come from beans (D3).

### D2. One bean pair per connection, registered by name
A `BeanDefinitionRegistryPostProcessor` reads the bound connection names through `Binder` and registers `<name>ClusterConnection` and `<name>Transport`, each with a qualifier of the connection's name. One connection is primary.
- **Rejected: a single `OpenBrokerConnections` registry bean.** It makes injection by name stringly typed and hides lifecycle.

An application bean with the same name wins (`@ConditionalOnMissingBean` semantics by name). `BrokerDrivers.load()` is one bean, closed last.

### D3. Function-valued options come from beans
| Option | Source |
|---|---|
| `TlsProvider` | the `SslBundles` bean, by `ssl-bundle` name; asked on every connect, so bundle reloads apply |
| `CredentialSource` | a bean qualified by the connection name, else the bound username and password or token |
| `CallInterceptor` | a bean, if present |
| context propagation | Micrometer `ContextSnapshotFactory`, when `io.micrometer:context-propagation` is on the class path |
| `MessagingObserver` | `openbroker-opentelemetry`'s observer when an `OpenTelemetry` bean exists |
| `Clock` | a `Clock` bean, if present |

### D4. Health
`OpenBrokerHealthContributor` is a composite named `openbroker`, with one `HealthIndicator` per connection. It calls the connection's health with a deadline (default 2 s) and maps the SDK's status to up, down or out of service. Details are the node count, the healthy nodes and the error reason.

### D5. Metrics
- **Micrometer.** A `MeterBinder` per connection registers the SDK's standard metric keys (ADR 0007), tagged with `openbroker.connection` and `openbroker.system`.
- **OpenTelemetry.** The observer emits spans and metrics directly. When both are present, both are used, because they are different signals.

### D6. Lifecycle
A `SmartLifecycle` at a late phase owns the processors that the application registers through an `OpenBrokerProcessors` helper bean:
- it starts them after the context is ready;
- on stop it calls `stop(grace)`;
- connections close in a later destroy phase.
- **Rejected: starting processors in bean init.** A message could arrive before the rest of the context is ready.

### D7. Framework-free enforcement
The Maven Enforcer `bannedDependencies` rule bans `org.springframework*` in every module except the two Spring modules. A test asserts the rule's module list.

### D8. Module layout
- `openbroker-spring-boot-autoconfigure` imports `spring-boot-dependencies` in its own dependency management, never in the parent. Spring Boot, Actuator, Micrometer and OpenTelemetry are optional dependencies there. It runs `spring-boot-configuration-processor` for metadata.
- `openbroker-spring-boot-starter` is a pom of the autoconfigure module plus `openbroker-support`.

Drivers are added by the application.

### D9. ADRs
- 0026 the starter binds the SDK's records and takes functions from beans (D1, D3);
- 0027 Spring lives only in the Spring modules (D7).

## Risks / Trade-offs

- [A record changed to bind breaks API users] → It is a minor bump while `0.x`, called out in the commit.
- [Bean names collide with application beans] → Names carry the `ClusterConnection` and `Transport` suffixes, and application beans win.
- [The Spring Boot release cadence outpaces the SDK] → The BOM pins only OpenBroker's modules. The autoconfigure module is tested against the newest Boot 4 release in CI.
