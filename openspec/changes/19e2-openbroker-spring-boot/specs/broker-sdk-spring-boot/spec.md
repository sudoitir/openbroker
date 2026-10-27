## Purpose

Defines how a Spring Boot application gets OpenBroker's clients as beans, from its own configuration, from Spring Boot's Kafka and Artemis connection details, or at runtime, with health, metrics, tracing and an orderly lifecycle, without Spring reaching the API or the drivers.

## ADDED Requirements

### Requirement: Connections are declared in configuration
An application SHALL be able to declare each connection under `openbroker.connections.<name>`, with its system, cluster, credentials and options. Each option SHALL bind to the SDK's own record where the SDK has one, and every component left out SHALL take the record's own default. Credentials, a closed set of forms, SHALL bind through one properties type that names the form.

#### Scenario: A Kafka connection in YAML
- **WHEN** an application declares a connection `orders` with the system `kafka`, two bootstrap endpoints and a SASL mechanism
- **THEN** a `ClusterConnection` and a `Transport` named `orders` are beans, and connecting uses those values

#### Scenario: A partial options block
- **WHEN** a connection sets only `sender.guarantee`
- **THEN** the connection's sender options carry that guarantee and every other value from `SenderOptions.defaults()`

#### Scenario: Every key is described
- **WHEN** an IDE reads the starter's configuration metadata
- **THEN** every bindable key path has a description, and no key is listed that does not bind

### Requirement: Spring Boot's own connection details are reused
When Spring Boot provides Kafka or Artemis connection details (from its properties, a service connection or Docker Compose) and the matching driver is present, the starter SHALL derive a connection named after the system from them, so that the application configures nothing twice. Only what Spring's details state SHALL be derived. An `openbroker.connections` entry of the same name SHALL be laid over the derived connection, and derivation SHALL be able to be turned off.

#### Scenario: Kafka with no OpenBroker configuration
- **WHEN** an application has `spring.kafka.bootstrap-servers` and the Kafka driver, and no `openbroker.*` key
- **THEN** a connection `kafka` is a bean that connects to those servers with Spring's security protocol and SSL bundle

#### Scenario: SCRAM credentials from Spring's Kafka properties
- **WHEN** `spring.kafka.properties` sets `sasl.mechanism` to `SCRAM-SHA-512` and a `sasl.jaas.config` with a username and password
- **THEN** the derived connection authenticates with that username and password, and neither appears in any log or failure

#### Scenario: A mechanism that cannot be derived
- **WHEN** Spring's Kafka properties use `OAUTHBEARER` and the application supplies no OpenBroker credentials
- **THEN** the application fails to start, and the message says to set the connection's credentials or a `CredentialSource` bean

#### Scenario: Artemis from a service connection
- **WHEN** a test declares an Artemis container with `@ServiceConnection` and the Artemis driver is present
- **THEN** a connection `artemis` sends and processes messages over Core, and its management facets report unavailable with a reason

#### Scenario: Embedded Artemis is not derived
- **WHEN** Spring's Artemis mode is embedded
- **THEN** no `artemis` connection is derived

#### Scenario: An overlay adds what Spring cannot say
- **WHEN** `openbroker.connections.artemis.cluster` adds a Jolokia endpoint to a derived connection
- **THEN** the connection uses Spring's Core endpoint and credentials and the added management endpoint

#### Scenario: Derivation is turned off
- **WHEN** `openbroker.spring-connections.enabled` is `false`
- **THEN** no connection is derived from Spring's details

### Requirement: A wrong connection fails the start
A connection whose driver is missing, whose option is not in the driver's schema, whose value fails validation, or which sets in configuration what only code can set, SHALL fail the application's start through Spring Boot's failure analysis, naming the connection and the key. A sensitive value SHALL never appear in that failure.

#### Scenario: A misspelt option
- **WHEN** a connection sets an option that its driver's schema does not have
- **THEN** the application fails to start, and the message names the connection and the option

#### Scenario: An out-of-range value
- **WHEN** a connection sets `processor.concurrency` to 1000
- **THEN** the application fails to start, and the message names the connection, the key and the limit

#### Scenario: A dead-letter target in YAML
- **WHEN** a connection sets `processor.retry.dead-letter`
- **THEN** the application fails to start, and the message says the key is set in code

#### Scenario: A sensitive option is invalid
- **WHEN** a sensitive option has an invalid value
- **THEN** the failure names the key and never the value

### Requirement: Clients are beans by name
Each configured or derived connection SHALL give one `ClusterConnection` bean and one `Transport` bean, qualified by the connection's name. With exactly one connection, its beans SHALL be primary. An application that defines its own bean of the same name SHALL keep it, and the starter SHALL back off.

#### Scenario: Two connections
- **WHEN** an application declares `orders` and `billing`
- **THEN** it can inject each one's transport by its qualifier, and an unqualified injection fails with a message naming both

#### Scenario: An application bean wins
- **WHEN** an application defines a bean named `ordersTransport`
- **THEN** the starter registers no transport for `orders`

### Requirement: What cannot be a property comes from beans
TLS SHALL come from a Spring SSL bundle; interception, the clock and option customizers from beans; credentials from properties or a `CredentialSource` bean qualified by the connection's name, which wins; context propagation from Micrometer when present. Each SHALL be asked for on every (re)connect.

#### Scenario: A rotated SSL bundle
- **WHEN** the SSL bundle a connection names is reloaded with a new certificate
- **THEN** the SDK is told of the change, and the next connection to a broker uses the new material

#### Scenario: A missing SSL bundle
- **WHEN** a connection names an SSL bundle that does not exist
- **THEN** connecting fails as unavailable with the TLS reason, not sent, and the message names the bundle

#### Scenario: A credential source bean
- **WHEN** an application defines a `CredentialSource` bean qualified `orders` and also sets credentials in properties
- **THEN** connection `orders` uses the bean

### Requirement: Runtime connections use the same building blocks
An application that opens connections at runtime from its own store SHALL get `BrokerDrivers`, the assembled connect options (beans, observers, client defaults and customizers) and SSL-bundle TLS from the starter, without declaring those connections in configuration. The starter SHALL NOT attach health or lifecycle to a connection it did not create.

#### Scenario: A connection opened at runtime
- **WHEN** an application asks the starter for the connect options of a connection named `c1` of system `artemis` and connects through the `BrokerDrivers` bean
- **THEN** the connection carries the application's interceptor, clock, observers tagged `c1`, and the `openbroker.client` timeouts

#### Scenario: A runtime connection is down
- **WHEN** a runtime connection's brokers stop answering
- **THEN** the application's health is unchanged

### Requirement: Health reflects each connection
With Actuator present, every configured or derived connection SHALL contribute a health indicator under `openbroker`: up when the cluster answers within a timeout, even partially; down with the reason otherwise; out of service while closing. A check SHALL never block past its timeout, at most one SHALL run per connection, and its details SHALL never carry credentials, endpoints or option values.

#### Scenario: A cluster stops answering
- **WHEN** a connection's brokers stop answering
- **THEN** its health turns down within the timeout, with the reason, and the other connections' health is unchanged

#### Scenario: One node of three is down
- **WHEN** one node of a three-node cluster does not answer
- **THEN** the connection is up, and the details name that node with its status and reason

#### Scenario: A check hangs
- **WHEN** a health check is still running and health is asked again
- **THEN** no second check starts, and the last result is returned marked stale

### Requirement: Telemetry follows the application's
Client metrics SHALL go to Micrometer when it is present, tagged with the connection's name and system, and to OpenTelemetry only otherwise; spans SHALL go to OpenTelemetry when it is present. Writing trace context into messages SHALL be able to be turned off. Without either, nothing SHALL be allocated per message.

#### Scenario: A traced application
- **WHEN** an application with an `OpenTelemetry` bean sends and processes a message through a connection
- **THEN** the send and the process are spans of one trace, with the messaging attributes

#### Scenario: Both Micrometer and OpenTelemetry
- **WHEN** an application has both a `MeterRegistry` and an `OpenTelemetry` bean
- **THEN** the messaging metrics are recorded once, in Micrometer, and the spans in OpenTelemetry

#### Scenario: A tool that relays messages
- **WHEN** `openbroker.tracing.inject-context` is `false` and the application sends a message
- **THEN** the send is a span, and the message carries no trace context written by the SDK

### Requirement: Processors follow the application's lifecycle
Processors registered by the application through the starter SHALL start only after the context is ready, and on shutdown SHALL stop within their grace time before their connections close. A connection SHALL close after every processor and sender that uses it, and the drivers SHALL close last.

#### Scenario: Graceful shutdown
- **WHEN** the application shuts down while handlers are running
- **THEN** in-flight handlers finish within the grace time, the rest are handed back uncounted, and only then do the connections close

#### Scenario: No message before the context is ready
- **WHEN** a processor is registered while the context is starting and messages are waiting
- **THEN** no handler runs before the context is ready
