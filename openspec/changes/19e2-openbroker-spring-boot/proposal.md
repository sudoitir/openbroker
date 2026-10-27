## How to run this change
This change is fully designed: `design.md` holds the decisions, with the alternatives weighed, and `tasks.md` holds the steps. Run it in a fresh session:

1. `git fetch`, then create a git worktree on a new branch off `origin/main`, and remove it after the merge.
2. Read this proposal, its specs, `design.md`, the `broker-sdk` and `broker-sdk-messaging` capabilities, and ADRs 0008, 0015 and 0016.
3. Re-check with ctx7 the latest Spring Boot 4 release, the Actuator health contributor API, configuration-properties binding of records, SSL bundles and Micrometer's context propagation. If a fact in `design.md` no longer holds, fix it with `/opsx:update` before applying. Ask only what is really the maintainer's to decide.
4. `/opsx:apply` with the harness under **Execution**. Each decision in `design.md` gets its ADR. An options record that does not bind is fixed in the SDK, never wrapped.
5. Verify: the auto-configuration's context-runner tests; one Testcontainers application test per driver; the framework-free check on `openbroker-api` and every driver; japicmp.
6. PR, merge on green CI, `/opsx:archive`, and release the next minor version.

## Why
Most JVM services that would use OpenBroker are Spring Boot applications. Today each one writes the same glue:
- building `ConnectRequest`s from its own properties;
- wiring TLS, tracing and health;
- closing clients in the right order on shutdown.

A starter makes a connection a few lines of `application.yml` and gives the clients as beans, with Actuator health and Micrometer or OpenTelemetry. `openbroker-api` and the drivers stay framework-free.

## What Changes
- Two new modules, released with the SDK and listed in the BOM:
  - `openbroker-spring-boot-autoconfigure`
  - `openbroker-spring-boot-starter`
- `openbroker.connections.<name>` in configuration binds directly to the SDK's own option records:
  - the system;
  - the cluster: nodes, endpoints and the driver's options;
  - credentials;
  - connect options;
  - default sender, receiver and processor options.
- Each connection becomes a `ClusterConnection` bean and a `Transport` bean, qualified by name. A single connection is primary.
- TLS comes from Spring SSL bundles. Credentials come from properties or a `CredentialSource` bean. Call interception comes from a bean. Tracing goes through `openbroker-opentelemetry` when an `OpenTelemetry` bean exists. Context propagation goes through Micrometer when it is present.
- Every connection gets an Actuator health contributor, and its metrics go to Micrometer.
- Processors that the application registers start after the context is ready and drain within their grace time on shutdown.
- The driver's option schema is checked at startup, so a misspelt option fails the start with its name.
- A connection is derived from Spring Boot's own Kafka and Artemis connection details (properties, service connections, Docker Compose), so an application configures nothing twice. An `openbroker.connections` entry of the same name adds what Spring cannot say.
- Applications that open connections at runtime get the same building blocks as beans: `BrokerDrivers`, the assembled connect options and SSL-bundle TLS.
- The SDK closes three gaps for every consumer:
  - connect refuses options the driver's schema does not declare (breaking);
  - observers compose;
  - the OpenTelemetry observer can trace sends without writing trace context into the message.

## Capabilities
### New Capabilities
- `broker-sdk-spring-boot`: Spring Boot auto-configuration and starter for OpenBroker connections, health, metrics, tracing and lifecycle.

### Modified Capabilities
- `broker-sdk`: the framework-free rule names the Spring modules as the only place Spring may appear, the release lists them as optional modules, and connecting validates options against the driver's schema.
- `broker-sdk-messaging`: observers compose, and trace injection into messages can be turned off.

## Out of scope
- An annotation-driven listener model (for example `@OpenBrokerListener`). A later change may add it on top of this one.
- Spring for Apache Kafka, Spring AMQP or Spring JMS integration. The starter exposes OpenBroker's own clients.
- Spring Boot 3.

## Depends on
- 19e-openbroker-kafka (the release it first ships with binds Artemis and Kafka)

## Execution
**Inline**: two small modules with one design, tested with the context runner.

Drivers touched: none; the starter is driver-neutral. The API breaks in one place: connect refuses options its driver does not declare. That takes the minor bump to `0.4.0`.

## Impact
New modules `openbroker-spring-boot-autoconfigure` and `openbroker-spring-boot-starter`; the BOM; the parent pom (Spring Boot's dependency management imported only in these modules); `ci.yml`; README; ADRs. Possibly small fixes to options records in `openbroker-api` so that they bind.
