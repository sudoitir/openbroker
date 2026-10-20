## ADDED Requirements

### Requirement: Delivery guarantees are declared and kept
Senders and receivers SHALL be created with a delivery guarantee: at most once, at least once, or transacted. A driver SHALL refuse a guarantee it cannot honour with `UNIMPLEMENTED`, and SHALL keep the one it accepted through reconnects and failovers.

#### Scenario: At least once across a failover
- **WHEN** a receiver at least once has unsettled messages and the live node fails over to its backup
- **THEN** every message is delivered again or was settled, and none is lost

### Requirement: Sends can be idempotent
A sender SHALL accept a deduplication id per message and SHALL use the broker's deduplication where the system has one, declaring the window. Where it has none, the capability SHALL be declared unavailable.

#### Scenario: A retried send
- **WHEN** a send is retried after an in-doubt failure with the same deduplication id
- **THEN** the destination holds the message once

### Requirement: Receiving is flow-controlled
Receivers SHALL take credit, and the driver SHALL never buffer more unsettled messages than the credit allows. A sender whose buffer is full SHALL return a typed outcome rather than grow without bound.

#### Scenario: A slow consumer
- **WHEN** a consumer settles slower than messages arrive
- **THEN** memory use stays within the configured credit and buffer

### Requirement: The client is observable without a dependency
The SDK SHALL emit OpenTelemetry messaging spans and metrics through hooks and SHALL carry trace context in message headers, without `openbroker-api` depending on OpenTelemetry.

#### Scenario: A traced send and receive
- **WHEN** a traced service sends a message and another service receives it
- **THEN** the receive span is linked to the send span

### Requirement: Performance has budgets
The SDK SHALL keep JMH benchmarks with budgets for throughput, latency and allocations per message, run in CI.

#### Scenario: A change allocates per message
- **WHEN** a change adds an allocation per message beyond the budget
- **THEN** the benchmark job fails
