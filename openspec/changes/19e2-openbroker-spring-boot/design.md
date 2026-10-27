# Design: the Spring Boot starter

## Context

The SDK's options are immutable records with documented defaults, validated in their compact constructors, using durations, counts, byte sizes and enums:
- `SenderOptions`, `ReceiverOptions` and `ProcessorOptions`;
- `RetryPolicy`, `FlowControl`, `SendBuffer`, `Backoff` and `Recovery`;
- `ClusterSpec(nodes, clusterEndpoints, Map<String,String> options)`, whose options are described by the driver's `ConnectionSchema` of `OptionSpec`s.

`ConnectOptions` is one record (`defaults()`, `withX`) that mixes bindable values with function-valued ones:
- bindable: `Timeouts` (connect, call), `maxResponseBytes`, the spool directory;
- function-valued: `CallInterceptor`, `TlsProvider`, `Clock`, context propagation (`UnaryOperator<Runnable>`), `MessagingObserver`.

`Credentials` is sealed (`UsernamePassword`, `BearerToken`, `None`), and `CredentialSource.forPlane(Plane)` supplies them. `TlsProvider.current()` gives `TlsMaterial(id, SSLContext, hostnameVerification, …)`, and `onChange` signals a rotation. `openbroker-opentelemetry` provides `OpenTelemetryMessaging`, an observer and link listener that emits spans and metrics.

Spring Boot 4.1.2 facts, checked with ctx7 and the 4.1.2 sources:
- Auto-configurations are listed in `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`.
- Records bind by constructor. A missing component binds as `null`, a missing `Optional` binds as `null` too, and defaults need `@DefaultValue` on the constructor parameter.
- `HealthIndicator` and `CompositeHealthContributor` are in `org.springframework.boot.health.contributor`.
- `SslBundles.getBundle(name)` returns the current bundle after a `reload-on-update` reload. `addBundleUpdateHandler(name, handler)` signals the reload.
- `KafkaConnectionDetails` (`org.springframework.boot.kafka.autoconfigure`) gives the bootstrap servers, an `SslBundle` and the security protocol, per client kind (`getAdmin()`, …). SASL is not part of it; it lives in `spring.kafka.properties`.
- `ArtemisConnectionDetails` (`org.springframework.boot.artemis.autoconfigure`) gives the mode (`NATIVE` or `EMBEDDED`), the broker URL, the user and the password.
- Testcontainers `@ServiceConnection` and Docker Compose create both details beans.

What the code at `0.3.0` lacks:
- a health call on `ClusterConnection`;
- a way to combine observers;
- any check of `ClusterSpec.options` against the driver's schema, so a misspelt option is silently ignored;
- a way to trace sends without writing trace context into the message.

## Goals / Non-Goals

**Goals:**
- One `application.yml` block per connection, binding the SDK's own records.
- Zero OpenBroker configuration where Spring Boot already knows the Kafka or Artemis service.
- The same building blocks for connections an application opens at runtime from its own store.
- Health, metrics and tracing that need no code.
- An orderly shutdown.
- Spring Boot's own conventions for third-party starters throughout.

**Non-Goals:**
- A listener annotation model.
- Integration with Spring Kafka, Spring AMQP or Spring JMS. Their clients coexist with OpenBroker's.
- Spring Boot 3.
- Broker resource metrics (`MetricKeys`) as gauges.
- Native images.

## Decisions

### D1. Bind the SDK's records by component, over their own defaults
`OpenBrokerProperties` holds `Map<String, Connection>`. `Connection` has these parts:

| Part | Type |
|---|---|
| `system` | the system name |
| `cluster` | `ClusterSpec` |
| `credentials` | `CredentialsProperties` |
| `options` | `ConnectionOptions` |
| `sender` | `SenderOptions` |
| `receiver` | `ReceiverOptions` |
| `processor` | `ProcessorOptions` |

Spring's constructor binding would pass `null` for every component the YAML leaves out, and the SDK records reject `null`. So a partial block (`sender.guarantee` alone) would fail.

`RecordBinder` binds a record component by component through Spring's `Binder`:
- **Keys:** each key is the component's kebab-case name.
- **Defaults:** a component that is not set takes the value of the record's own default (`SenderOptions.defaults()`, `ProcessorOptions.defaults()`, `ReceiverOptions.consume()`). A nested record recurses with the parent default's value as its base.
- **`Optional<X>`:** the binder binds `X` and wraps it.
- **Interface-typed components:** a component typed by an interface (the sealed `SendTarget` dead-letter target) cannot come from configuration. Setting one fails the start with "set it in code".
- **Validation:** a compact-constructor violation (for example `concurrency` above 64) fails the start. The failure names the connection and the key.

Two types are the starter's own:
- `CredentialsProperties` names the form (`username-password`, `bearer-token` or `none`) and maps to the sealed `Credentials`. Secrets go straight into `Secret`.
- `ConnectionOptions` covers what YAML can say of `ConnectOptions`: `connect-timeout`, `call-timeout`, `max-response-bytes`, `spool-directory`, `ssl-bundle` and `verify-hostname`. Every key is laid over the starter's client defaults (D10).

The configuration processor cannot see through `RecordBinder`, so the module ships `additional-spring-configuration-metadata.json` with a description and default for each key. A test asserts that the file lists exactly the bindable key paths.

Rejected:
- **Binding the records directly.** Partial blocks break.
- **A parallel set of JavaBean properties classes.** It doubles every option and drifts.
- **`@DefaultValue` on the SDK records.** That puts Spring into the API.

### D2. Connect refuses options the driver does not declare
`ConnectionSchema.validate(Map<String,String>)` returns the violations:
- an unknown key;
- a required key that is missing;
- a value of the wrong type;
- a value outside the allowed values.

A sensitive value is never quoted. `BrokerDrivers.connect` and `verify` throw `INVALID_ARGUMENT` (`NOT_SENT`) with the violations, so every consumer fails fast, not only Spring applications. The starter runs the same check while binding, before any network I/O, and reports it through a `FailureAnalyzer`.

This is a breaking behaviour change (`feat(api)!`). Both drivers declare every option they read.

### D3. One bean pair per configured connection, registered by name
A `BeanDefinitionRegistryPostProcessor` reads the connection names through `Binder` and registers two beans per connection:
- `<name>ClusterConnection`;
- `<name>Transport`, when the driver offers messaging.

Each bean is qualified by the connection's name. With exactly one connection, its beans are primary. An application bean of the same name wins, and the starter backs off.

Rejected: **a single registry bean.** Injection by name becomes stringly typed, and the bean hides lifecycle.

### D4. Function-valued options come from beans
| Option | Source |
|---|---|
| `TlsProvider` | `SslBundleTls`, by the connection's `ssl-bundle` name; current on every connect, and `onChange` fires on a bundle reload |
| `CredentialSource` | a bean qualified by the connection's name, else the bound credentials |
| `CallInterceptor` | a bean, if present |
| context propagation | Micrometer's `ContextSnapshotFactory`, when `io.micrometer:context-propagation` is on the class path |
| `MessagingObserver` | D6 |
| `Clock` | a `Clock` bean, if present |

Every connection's `ConnectOptions` then passes through the ordered `ConnectOptionsCustomizer` beans, in the same way `RestClientCustomizer` works.

### D5. Health from the cluster's own description
`openbroker` is a composite with one `HealthIndicator` per configured connection. The indicator calls `nodes().describe(Scope.whole())` on a virtual thread, with a deadline (`openbroker.health.timeout`, default 2 s).

| Status | When |
|---|---|
| UP | The cluster answers. A `Partial` counts as an answer; the details list each failed node with its status and reason. A cluster that serves must not flap readiness. |
| DOWN | No node answers, or the deadline passes. The reason comes with it. |
| OUT_OF_SERVICE | The connection is closing. |

At most one check runs per connection. While a check is in flight, the indicator returns the last result, marked stale. Details carry counts, node ids and reasons, never endpoints, options or credentials.

The composite is behind `@ConditionalOnEnabledHealthIndicator("openbroker")`.

### D6. Micrometer owns metrics, OpenTelemetry owns traces
**The Micrometer observer.** `MicrometerMessagingObserver` records the same instruments as the OpenTelemetry observer:
- `messaging.client.operation.duration`;
- `messaging.client.sent.messages`;
- `messaging.client.consumed.messages`;
- `messaging.process.duration`;
- `openbroker.messaging.link.recoveries`.

Its tags are `openbroker.connection`, `openbroker.system`, the operation and the destination. It watches the transport for link recoveries.

**Which observer runs:**
- **A `MeterRegistry` is present:** the Micrometer observer records the metrics. Then:
  - **An `OpenTelemetry` bean is also present:** the OpenTelemetry observer gets a view of it with a no-op meter provider. It emits spans and no metrics, so nothing is counted twice. That matches Boot's own convention: metrics through Micrometer, OTLP export through Micrometer's registry.
- **Only `OpenTelemetry` is present:** its observer emits both spans and metrics.
- **Neither is present:** `MessagingObserver.none()`, which allocates nothing per message.

The SDK gains `MessagingObserver.composite(MessagingObserver...)`, which is additive.

### D7. Trace sends without writing into the message
Some tools move, retry or relay messages that producers wrote. Those messages must keep their properties, including the producer's trace context. `OpenTelemetryMessaging.builder(OpenTelemetry).injectContext(boolean).build()` still spans sends but writes nothing into the message when it is off. `of(OpenTelemetry)` stays the default with injection on. The starter maps the flag to `openbroker.tracing.inject-context` (default true).

### D8. Runtime connections use the same building blocks
Applications that open connections at runtime from their own store get the beans that configured connections use:
- **`BrokerDrivers`.** Closed last.
- **`OpenBrokerConnectOptions.forConnection(name, system, tls)`.** It returns `ConnectOptions` assembled from:
  - the beans (D4);
  - the observer (D6), tagged with the name and system;
  - the client defaults, `openbroker.client.*`: `connect-timeout`, `call-timeout`, `max-response-bytes` and `spool-directory`, with the SDK's own defaults as fallbacks;
  - the customizers.
- **`OpenBrokerConnectOptions.linkListener(name, system)`** returns the link listener matching those observers (the Micrometer recoveries counter and/or the OpenTelemetry observer), or a no-op one; a runtime connection passes it to `transport.watch`, and configured connections do so themselves.
- **`SslBundleTls.forBundle(bundle, verifyHostname)`** and **`forBundle(SslBundle, verifyHostname)`.**
  - Each returns a `TlsProvider` whose material is current on every connect and whose `onChange` fires on a reload.
  - A missing bundle is a `BrokerException`: `UNAVAILABLE`, reason `TLS_HANDSHAKE_FAILED`, `NOT_SENT`. Its message names the bundle and says to add it under `spring.ssl.bundle`.

Health and the processor lifecycle are not applied to runtime connections. A target the application manages must not decide the application's own health or shutdown order, so registering one is the application's call.

### D9. Connections derived from Spring Boot's Kafka and Artemis details
An application that already configures `spring.kafka.*` or `spring.artemis.*`, or gets them from `@ServiceConnection` or Docker Compose, configures nothing again. The source is Boot's `ConnectionDetails` beans, not the property classes, so service connections work too.

**`kafka`.** Derived from `KafkaConnectionDetails.getAdmin()` when `openbroker-kafka` is present:

| Connection part | Source |
|---|---|
| endpoints | each bootstrap server becomes `Endpoint(DATA, "kafka", host:port)` |
| `security.protocol` | the details' security protocol |
| TLS | the details' `SslBundle`, asked again on every connect |
| `sasl.mechanism` | `spring.kafka.properties` |

- **Credentials for `PLAIN` and `SCRAM-*`:** the username and password are read from `sasl.jaas.config` with kafka-clients' own `JaasConfig` parser.
- **Other mechanisms:** they cannot be derived honestly. They need `credentials` or a `CredentialSource` bean, and the start fails saying so.

**`artemis`.** Derived from `ArtemisConnectionDetails` in `NATIVE` mode when `openbroker-artemis` is present:
- **Endpoints:** the Core endpoints on the `DATA` plane come from the broker URL.
- **Credentials:** the user and password become `DATA`-plane credentials.
- **Management:** Spring knows no management endpoint, so the driver reports the management facets as unavailable until the connection adds the Jolokia endpoint and `MANAGEMENT` credentials.
- **`EMBEDDED` mode:** not derived, because it has no URL.

**Overlay and opt-out:**
- An `openbroker.connections.kafka` or `openbroker.connections.artemis` entry is laid over the derived connection key by key. Lists replace.
- `openbroker.spring-connections.enabled=false` turns derivation off.

**Where it lives:**
- Per-system nested configurations sit behind `@ConditionalOnClass` of Boot's details type and the driver.
- They come `@AutoConfiguration(after = …)` Boot's Kafka and Artemis auto-configurations.
- Beans are registered from the details' bean definitions by type, and resolved lazily.
- `spring-boot-kafka`, `spring-boot-artemis`, both drivers and kafka-clients are optional dependencies of the autoconfigure module.

Rejected:
- **Reading `KafkaProperties` or `ArtemisProperties`.** It misses service connections and Docker Compose.
- **Bridging into `KafkaTemplate` or Spring JMS.** It is out of scope.

### D10. Lifecycle
A `SmartLifecycle` at a late phase owns the processors that the application registers through the `OpenBrokerProcessors` bean:
- it starts them after the context is ready, because `transport.process` starts a processor immediately;
- on stop it calls `stop(grace)` with each processor's `stopGrace`;
- configured connections close in a later destroy phase, and `BrokerDrivers` closes after them.

Rejected: **starting processors in bean init.** A message could arrive before the rest of the context is ready.

### D11. Spring only in the Spring modules
The Maven Enforcer's `bannedDependencies` bans `org.springframework*` and `org.springframework.boot*` in every module except the two Spring modules. A test profile adds Spring to `openbroker-api`, and the build must fail.

### D12. Module layout and Boot conventions
- **`openbroker-spring-boot-autoconfigure`** holds all the code:
  - it imports `spring-boot-dependencies` 4.1.2 in its own `dependencyManagement`, never in the parent;
  - Boot's actuator, Micrometer, context-propagation, OpenTelemetry, `openbroker-opentelemetry`, the Kafka and Artemis Boot modules and the drivers are optional;
  - it runs `spring-boot-configuration-processor` for `ConnectionOptions`, `CredentialsProperties` and the starter's own keys.
- **`openbroker-spring-boot-starter`** is a pom of the autoconfigure module plus `openbroker-support`. Applications add the drivers they use.
- **Boot conventions:**
  - the third-party naming (no `spring-boot-` prefix);
  - every bean `@ConditionalOnMissingBean`;
  - optional integrations in nested configurations behind `@ConditionalOnClass`/`@ConditionalOnBean`;
  - a `FailureAnalyzer` for a wrong connection;
  - `ApplicationContextRunner` tests for each condition, with `FilteredClassLoader` for each absent integration.

### D13. ADRs
- 0026: the starter binds the SDK's records by component over their own defaults, and takes functions from beans (D1, D4, D8).
- 0027: Spring lives only in the Spring modules (D11).
- 0028: connect validates options against the driver's schema (D2).
- 0029: Micrometer owns metrics and OpenTelemetry owns traces, with composable observers and optional trace injection (D6, D7).
- 0030: connections are derived from Spring Boot's connection details, with an overlay (D9).

## Risks / Trade-offs

- **Connect now refuses undeclared options, which breaks callers that passed them.** It is a minor bump while `0.x`, called out with `BREAKING CHANGE:`.
- **Bean names could collide with application beans.** The names carry the `ClusterConnection` and `Transport` suffixes, and application beans win.
- **A health check costs a cluster read.** It is bounded by the deadline and by the one check in flight per connection.
- **Spring Boot's release cadence outpaces the SDK's.** The BOM pins only OpenBroker's modules. The autoconfigure module is built and tested against the newest Boot 4 release.
