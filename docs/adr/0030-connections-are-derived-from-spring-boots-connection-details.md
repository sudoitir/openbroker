# ADR-0030: Connections are derived from Spring Boot's connection details

- **Status**: accepted
- **Date**: 2026-10-27
- **Deciders**: Mahdi Amirabdollahi

## Context

Applications that use Kafka or Artemis already configure them for Spring Boot (`spring.kafka.*`, `spring.artemis.*`),
or get them from Testcontainers `@ServiceConnection` or Docker Compose. Spring Boot expresses "where the service is" as
`KafkaConnectionDetails` and `ArtemisConnectionDetails` beans. Asking for the same servers, TLS and credentials again
under `openbroker.*` duplicates configuration that then drifts.

## Decision

When a details bean exists and the matching driver is present, the starter derives a connection named after the
system (`kafka`, `artemis`) from it, mapping only what the details state: bootstrap servers, security protocol and SSL
bundle for Kafka, with PLAIN or SCRAM credentials read from `sasl.jaas.config` by kafka-clients' own parser; the Core
URL, user and password for Artemis in native mode. What Spring cannot say (another SASL mechanism, Artemis's management
endpoint) is added by an `openbroker.connections` entry of the same name, laid over key by key, or the start fails
saying what is missing. `openbroker.spring-connections.enabled=false` turns derivation off.

## Consequences

- Zero OpenBroker configuration for the common case, including tests with service connections.
- A derived Artemis connection has no management plane until one is added; the driver reports those facets
  unavailable rather than pretending.
- The autoconfigure module carries per-system mapping code behind class conditions; the drivers stay framework-free.

## Alternatives considered

- Read `KafkaProperties` and `ArtemisProperties`: misses service connections and Docker Compose.
- Parse JAAS with a regular expression: fragile; kafka-clients already ships the parser.
- Bridge into Spring Kafka's `KafkaTemplate` or Spring JMS: a different client model, out of scope.
