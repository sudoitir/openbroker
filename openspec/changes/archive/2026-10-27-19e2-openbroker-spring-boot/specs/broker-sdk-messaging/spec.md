## MODIFIED Requirements

### Requirement: The client is observable without a dependency
The SDK SHALL emit OpenTelemetry messaging spans and metrics through hooks and SHALL carry trace context in message properties, without `openbroker-api` depending on OpenTelemetry. With no observer configured, observation SHALL allocate nothing per message. Observers SHALL compose, so that one connection can feed several. Writing trace context into sent messages SHALL be able to be turned off while sends are still traced, for tools that relay messages producers wrote.

#### Scenario: A traced send and receive
- **WHEN** a traced service sends a message and another service receives it
- **THEN** the receive span is linked to the send span

#### Scenario: Two observers
- **WHEN** a connection's observer is the composite of a metrics observer and a tracing observer
- **THEN** each send is seen by both, and an error in one does not stop the other or the send

#### Scenario: A relayed message keeps its producer's context
- **WHEN** a tool with trace injection off sends a message that already carries a `traceparent`
- **THEN** the send is a span, and the message's `traceparent` is the producer's, unchanged
