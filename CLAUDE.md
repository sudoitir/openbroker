# OpenBroker — project instructions

One Java SDK, and one vocabulary, for every message broker and queue service. Consumers connect
through `BrokerDrivers`; drivers implement the SPI and prove themselves against the TCK.
Apache-2.0, public.

## Layout

| Module | What it is | Depends on |
| --- | --- | --- |
| `openbroker-parent` | Build settings, release profile | – |
| `openbroker-bom` | Version alignment for adopters (no parent) | – |
| `openbroker-api` | `io.github.sudoitir.openbroker` (consumer API) and `.spi` (driver SPI) | JSpecify only |
| `openbroker-support` | Shared implementation for drivers and consumers | api |
| `openbroker-tck` | Abstract JUnit contract tests a driver extends | api, JUnit, AssertJ |
| `openbroker-artemis` | The Apache ActiveMQ Artemis driver (Jolokia + Core) | api, support, Artemis client, Jackson 3 |
| `openbroker-kafka` | The Apache Kafka driver (Admin, producer, consumer, share consumer) | api, support, Kafka clients |
| `openbroker-opentelemetry` | Traces and metrics for messaging through OpenTelemetry | api, OpenTelemetry API |
| `openbroker-spring-boot-autoconfigure` | Spring Boot 4 auto-configuration: connections, health, metrics, tracing, lifecycle | api; Spring Boot, the drivers and telemetry optional |
| `openbroker-spring-boot-starter` | Pom of the autoconfigure module and `openbroker-support`; applications add the drivers | autoconfigure, support |

## Non-negotiables

1. **Framework-free.** No Spring, no DI container, no logging framework (`System.Logger`).
   `openbroker-api` depends on nothing but JSpecify. The one exception is the two Spring modules
   (`openbroker-spring-boot-*`): Spring lives only there (ADR-0027), the Enforcer bans it everywhere else, and
   Spring Boot's versions are managed in the autoconfigure module alone.
2. **Neutral names.** Every public name must read right for Kafka, RabbitMQ, ActiveMQ Classic and
   Artemis, Pulsar, NATS JetStream, Solace, SQS, Azure Service Bus and Google Pub/Sub. A name shaped
   by one system or by JMS is a defect. Native behaviour goes through `unwrap`; native data is
   namespaced (`artemis.routing_type`).
3. **Closed sets are complete before 1.0.** `ResourceClass`, `Operation`, `Position`, `Status`,
   `Plane`, `Disposition` and the other sealed or enum sets are consumers' exhaustive `switch`es.
4. **Secure by default.** Secrets and sensitive options never reach logs, exceptions or
   `toString()`; TLS hostname verification on; no redirects; bounded responses; no Java
   deserialization of message bodies; filters parsed before use.
5. **Honest results.** Partial results name every node's outcome; totals say exact, estimate or
   unavailable; errors carry a gRPC status, an AIP-193 reason and whether the request was sent.
6. **No consumer is named.** Code, docs, ADRs, commits, PRs, issues and tags never mention a
   product that uses OpenBroker, or any private repository.
7. **Every driver passes the TCK** for the capabilities it declares, against a real broker.

## Process

- OpenSpec for every change (`openspec/`); ADRs in `docs/adr/` (Nygard, `000-template.md`).
- Library facts through `npx ctx7@latest`, never memory.
- SemVer. `0.x`: a breaking API change raises the minor version; japicmp checks it against
  `openbroker.previous.version`. `@Incubating` types are exempt. A maintainer releases by running the
  `Release` workflow with the version; it publishes to Maven Central and tags `vX.Y.Z` (ADR-0009).
- Commits: `.claude/rules/05-commits.md`.

## Commands

```bash
./mvnw verify                          # build, unit tests, TCK against Artemis (needs Docker)
./mvnw -pl openbroker-api test         # one module
./mvnw process-sources                 # format (Palantir; the BOM has no formatter, so not spotless:apply)
./mvnw -DskipTests install             # install a local SNAPSHOT for a consumer under development
```
