## Purpose

Defines how a Spring Boot application declares OpenBroker connections in configuration and receives the SDK's clients as beans, with health, metrics, tracing and an orderly lifecycle, without Spring reaching the API or the drivers.

## ADDED Requirements

### Requirement: Connections are declared in configuration
An application SHALL declare each connection under `openbroker.connections.<name>` with its system, cluster, credentials and options. Each part SHALL bind to the SDK's own record, with no separate properties type. A connection whose driver is missing, whose option is not in the driver's schema, or whose option fails validation SHALL fail the application's start, naming the connection and the option. A sensitive value SHALL never appear in that failure.

#### Scenario: A Kafka connection in YAML
- **WHEN** an application declares a connection `orders` with the system `kafka`, two bootstrap endpoints and a SASL mechanism
- **THEN** a `ClusterConnection` and a `Transport` named `orders` are beans, and connecting uses those values

#### Scenario: A misspelt option
- **WHEN** a connection sets an option that its driver's schema does not have
- **THEN** the application fails to start, and the message names the connection and the option

### Requirement: Clients are beans by name
Each connection SHALL give one `ClusterConnection` bean and one `Transport` bean, qualified by the connection's name. With exactly one connection, its beans SHALL be primary. An application that defines its own bean of the same name SHALL keep it, and the starter SHALL back off.

#### Scenario: Two connections
- **WHEN** an application declares `orders` and `billing`
- **THEN** it can inject each one's transport by its qualifier, and an unqualified injection fails with a message naming both

### Requirement: What cannot be a property comes from beans
TLS material SHALL come from a Spring SSL bundle named by the connection. Call interception SHALL come from a `CallInterceptor` bean. Credentials SHALL come from properties or from a `CredentialSource` bean, which wins. Context propagation SHALL use Micrometer's context propagation when it is present. All of these SHALL be asked for on every (re)connect, as the SDK requires.

#### Scenario: A rotated SSL bundle
- **WHEN** the SSL bundle a connection names is reloaded with a new certificate
- **THEN** the next connection to a broker uses it, and open links stay open

### Requirement: Health reflects each connection
With Actuator present, every connection SHALL contribute a health indicator under `openbroker`, up when the cluster answers within a configured timeout, down with the SDK's error status and reason otherwise, and out of service while closing. A health check SHALL never block longer than its timeout, and its details SHALL never carry credentials.

#### Scenario: A cluster stops answering
- **WHEN** a connection's brokers stop answering
- **THEN** its health turns down within the timeout, with the reason, and the other connections' health is unchanged

### Requirement: Telemetry follows the application's
With an `OpenTelemetry` bean present, every connection SHALL use the SDK's OpenTelemetry observer. With Micrometer present, every connection's standard metrics SHALL be registered as meters tagged with the connection's name and system. Without either, nothing SHALL be observed and nothing allocated per message.

#### Scenario: A traced application
- **WHEN** an application with an `OpenTelemetry` bean sends and processes a message through a connection
- **THEN** the send and the process are spans of one trace, with the messaging attributes

### Requirement: Processors follow the application's lifecycle
Processors registered by the application through the starter SHALL start only after the context is ready, and on shutdown SHALL stop within their grace time before their connections close. A connection SHALL close after every processor and sender that uses it.

#### Scenario: Graceful shutdown
- **WHEN** the application shuts down while handlers are running
- **THEN** in-flight handlers finish within the grace time, the rest are handed back uncounted, and only then do the connections close
