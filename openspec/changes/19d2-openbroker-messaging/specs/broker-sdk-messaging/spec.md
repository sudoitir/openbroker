## ADDED Requirements

### Requirement: Delivery guarantees are declared and kept
Every sender and receiver SHALL be opened with a delivery guarantee: at most once, at least once or transacted. The transport SHALL state statically which guarantees, ordering scopes, prefetch bounds and retry enforcements it supports, and a driver SHALL refuse a guarantee or ordering it cannot honour with `UNIMPLEMENTED`, not sent. A driver SHALL keep the guarantee it accepted through reconnects and failovers. At least once and transacted sends SHALL be durable, and an explicitly non-durable message SHALL be refused with `INVALID_ARGUMENT`.

#### Scenario: At least once across a failover
- **WHEN** a receiver at least once has unsettled messages and the live node fails over to its backup
- **THEN** every message is delivered again or was settled, and none is lost

#### Scenario: A guarantee the system cannot keep
- **WHEN** a consumer opens a transacted sender on a driver whose capabilities omit transacted
- **THEN** the call fails with `UNIMPLEMENTED`, not sent, and the capabilities had already said so

#### Scenario: At most once is never redelivered
- **WHEN** a receiver at most once is closed while holding messages it has not settled
- **THEN** no receiver ever gets those messages again

### Requirement: Loss and duplicates follow a stated contract
After a link is lost, the driver SHALL recover it with the link's backoff and SHALL report each change of state (open, recovering, failed, closed) to the transport's listeners, naming the node that serves the link. On recovery:
- unconfirmed at-least-once sends SHALL be sent again in their original order, and their receipts SHALL say so;
- at-most-once sends SHALL NOT be sent again, and their receipts SHALL fail as in doubt;
- settling a message received before the recovery SHALL fail with `DELIVERY_STALE`, and the message SHALL be delivered again;
- committing a transaction begun before the recovery SHALL fail with `ABORTED`, `TRANSACTION_ROLLED_BACK`;
- a commit whose answer never arrived SHALL be in doubt.
A link that cannot recover within its give-up time SHALL become failed, and every pending receipt SHALL fail with `UNAVAILABLE`, in doubt.

#### Scenario: A network partition heals
- **WHEN** the network to a node is cut while a sender at least once has unconfirmed messages, and is restored
- **THEN** the listener sees the link recovering and then open, and every accepted message is confirmed, with re-sent ones marked as such

#### Scenario: A failover is reported
- **WHEN** a link's node fails and its backup takes over
- **THEN** the listener sees the link open again, served by the backup

#### Scenario: A settle after recovery
- **WHEN** a consumer settles a message received before its link recovered
- **THEN** the settle fails with `DELIVERY_STALE`, not sent, and the message arrives again

### Requirement: Sends can be idempotent
A sender SHALL accept a deduplication id per message and SHALL use the broker's deduplication where the system has one, reporting the window per destination. An idempotent sender SHALL give a message without an id one of its own, so that its own re-sends are deduplicated. Where the system has no deduplication, the capability SHALL be declared unavailable and an idempotent sender SHALL be refused with `UNIMPLEMENTED`.

#### Scenario: A retried send
- **WHEN** a send is retried after an in-doubt failure with the same deduplication id
- **THEN** the destination holds the message once

#### Scenario: A broker killed during idempotent sending
- **WHEN** an idempotent sender at least once sends while its broker is killed and restarted
- **THEN** every accepted message is in the destination exactly once, in the order sent

### Requirement: Ordering has a declared scope
A sender and a processor SHALL state the ordering they need: none, per group, per partition, or the whole subscription. Messages of one scope SHALL reach the handler in the order sent, one at a time. A sender that needs ordering at least once SHALL be idempotent, or it SHALL be refused with `INVALID_ARGUMENT`.

#### Scenario: Groups keep their order under concurrency
- **WHEN** a processor ordered per group with four lanes handles three interleaved groups
- **THEN** each group's messages are handled in the order sent, and no two of one group run at once

### Requirement: Receiving is flow-controlled
Receivers SHALL take credit: the driver SHALL never hand out more unsettled messages than the credit allows, and SHALL buffer ahead no more than the prefetch bound the system enforces (messages or bytes, as declared). Zero prefetch SHALL buffer nothing.

#### Scenario: A slow consumer
- **WHEN** a consumer settles slower than messages arrive
- **THEN** memory use stays within the configured credit and buffer

#### Scenario: Unprefetched messages stay available
- **WHEN** a receiver with a credit of five and no prefetch holds five unsettled messages
- **THEN** another receiver on the same subscription receives the rest

### Requirement: Sending is bounded
A sender SHALL bound the messages it has accepted but not confirmed, by count and bytes. Offering a message to a full sender SHALL return a full outcome without blocking; a blocking send SHALL wait for room at most its configured time and then fail with `RESOURCE_EXHAUSTED`, `SEND_BUFFER_FULL`, not sent. A sender that is not transacted SHALL be safe to use from several threads.

#### Scenario: A sender cut off from its broker
- **WHEN** a sender with room for ten messages is cut off from its broker and keeps offering
- **THEN** ten offers are accepted, the next ones return full, and memory does not grow

### Requirement: Processing is concurrent and settles each message
A processor SHALL run a handler on virtual threads in a configured number of lanes, and SHALL settle each message with the handler's disposition independently of the others. A handler that throws SHALL count as a failed delivery. Stopping a processor SHALL let handlers in flight finish within the grace time and SHALL hand back the rest uncounted.

#### Scenario: Concurrency is a ceiling
- **WHEN** a processor with eight lanes receives a hundred messages whose handlers block
- **THEN** exactly eight handlers run at once

#### Scenario: Releasing one message
- **WHEN** one lane's handler releases its message while other lanes hold theirs
- **THEN** only that message is handed back

### Requirement: Retries and dead-lettering follow a policy
A processor SHALL take a retry policy. The system policy SHALL hand a failed message back counted and leave delay, attempts and dead-lettering to the system. A managed policy SHALL count attempts and wait the backoff between them: in place when processing is ordered, by a delayed release otherwise. After the last attempt it SHALL reject the message to the policy's dead-letter destination or the system's. A rejection SHALL dead-letter at once. The transport SHALL declare, for delay, attempts and dead-lettering, whether the system or the client enforces them, and a driver SHALL refuse a managed policy the system's own limits would cut short.

#### Scenario: A message that keeps failing
- **WHEN** a handler fails every time under a managed policy of three attempts with a backoff
- **THEN** it is called three times, at least the backoff apart, and the message lands in the dead-letter destination with the reason

#### Scenario: Ordered retries keep the order
- **WHEN** an ordered processor's handler fails twice on a group's first message and then succeeds
- **THEN** the group's later messages are handled after it, in order

### Requirement: A transaction can span a receive and its sends
Where the system declares joined transactions, a transacted receiver or delivery SHALL offer a sender in its own transaction, so that one commit settles the received messages and sends the new ones.

#### Scenario: Consume, transform, produce
- **WHEN** a transacted processor sends a result through its delivery's sender and commits, and the client dies before or after the commit
- **THEN** the result exists exactly when the input is settled

### Requirement: The client has asynchronous and blocking forms that suit virtual threads
Every blocking call SHALL have a non-blocking form or run correctly on a virtual thread. Drivers SHALL NOT pin virtual threads, SHALL NOT run consumer code or complete consumer futures on I/O threads, and SHALL carry the caller's context onto the threads that complete them.

#### Scenario: No pinning
- **WHEN** a processor handles messages on virtual threads while JFR records pinning
- **THEN** no virtual thread is pinned

### Requirement: Messaging is secure
Links SHALL authenticate with the current credentials and TLS material (including client certificates) on every open and recovery, without closing open links when either rotates. A refusal for one destination SHALL fail with `PERMISSION_DENIED`, `NOT_AUTHORIZED`, naming the destination in the error metadata, and SHALL leave the link open.

#### Scenario: Credentials rotate under load
- **WHEN** the credentials change while receivers are processing and their link is then cut
- **THEN** the receivers keep processing, and the recovered link uses the new credentials

#### Scenario: A destination the account may not use
- **WHEN** a sender offers to a destination the account may not send to
- **THEN** the receipt fails with `PERMISSION_DENIED`, `NOT_AUTHORIZED`, naming the destination, and later sends to permitted destinations work

### Requirement: The client is observable without a dependency
The SDK SHALL emit OpenTelemetry messaging spans and metrics through hooks and SHALL carry trace context in message properties, without `openbroker-api` depending on OpenTelemetry. With no observer configured, observation SHALL allocate nothing per message.

#### Scenario: A traced send and receive
- **WHEN** a traced service sends a message and another service receives it
- **THEN** the receive span is linked to the send span

### Requirement: Options are plain values
Every messaging option SHALL be an immutable record with documented defaults and validation, using durations, counts, byte sizes and enums only, so that it can be bound from configuration properties.

#### Scenario: Defaults are usable
- **WHEN** a consumer opens a sender, receiver and processor with default options on a driver that supports at least once
- **THEN** each opens, bounded, with at-least-once delivery

### Requirement: Performance has budgets
The SDK SHALL keep JMH benchmarks with budgets for throughput, latency and allocations per message, run in CI.

#### Scenario: A change allocates per message
- **WHEN** a change adds an allocation per message beyond the budget
- **THEN** the benchmark job fails

### Requirement: The TCK proves every guarantee, including under failure
The TCK SHALL test each declared guarantee, idempotence, ordering scope, flow-control bound, retry policy, joined transaction and security behaviour against a real broker. A chaos suite SHALL kill the broker mid-flight, partition the network and fail over an HA pair, checking the contract for loss and duplicates.

#### Scenario: A driver loses a message on failover
- **WHEN** a driver claims at least once and loses a confirmed message when the primary is killed
- **THEN** the chaos suite fails
