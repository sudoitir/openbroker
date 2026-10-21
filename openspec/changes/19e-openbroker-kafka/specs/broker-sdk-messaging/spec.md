## MODIFIED Requirements

### Requirement: Sends can be idempotent
A sender SHALL accept a deduplication id per message. The transport SHALL declare its deduplication scope: none, the sender's own session, or caller ids within a window, reporting the window per destination. An idempotent sender SHALL give a message without an id one of its own, so that its own re-sends are deduplicated in every declared scope. A sender that asks for a wider scope than the transport declares SHALL be refused with `UNIMPLEMENTED`, naming the scope offered. Where the scope is none, an idempotent sender SHALL be refused with `UNIMPLEMENTED`.

#### Scenario: A retried send
- **WHEN** a send is retried after an in-doubt failure with the same deduplication id on a transport with a caller-id window
- **THEN** the destination holds the message once

#### Scenario: A broker killed during idempotent sending
- **WHEN** an idempotent sender at least once sends while its broker is killed and restarted
- **THEN** every accepted message is in the destination exactly once, in the order sent

#### Scenario: A scope the system does not keep
- **WHEN** a sender asks for caller-id deduplication on a transport that declares only the session scope
- **THEN** it is refused with `UNIMPLEMENTED`, naming the session scope

### Requirement: Retries and dead-lettering follow a policy
A processor SHALL take a retry policy. Its default SHALL be the system policy where the transport declares the system enforces delay, attempts and dead-lettering, and a managed policy otherwise, so that default options open on every driver. The system policy SHALL hand a failed message back counted and leave delay, attempts and dead-lettering to the system. A managed policy SHALL count attempts and wait the backoff between them: in place when processing is ordered, by a delayed release otherwise. After the last attempt it SHALL reject the message to the policy's dead-letter destination or the system's. A rejection SHALL dead-letter at once. The transport SHALL declare, for delay, attempts and dead-lettering, whether the system or the client enforces them, per subscription kind, and a driver SHALL refuse a system policy the system does not enforce and a managed policy the system's own limits would cut short.

#### Scenario: A message that keeps failing
- **WHEN** a handler fails every time under a managed policy of three attempts with a backoff
- **THEN** it is called three times, at least the backoff apart, and the message lands in the dead-letter destination with the reason

#### Scenario: Ordered retries keep the order
- **WHEN** an ordered processor's handler fails twice on a group's first message and then succeeds
- **THEN** the group's later messages are handled after it, in order

#### Scenario: The default policy on a system without retries
- **WHEN** a processor opens with default options on a subscription whose system enforces no retries
- **THEN** it opens with a managed policy, and a failing message is retried and dead-lettered by the client

### Requirement: The TCK proves every guarantee, including under failure
The TCK SHALL test each declared guarantee, idempotence scope, ordering scope, flow-control bound, retry policy, joined transaction and security behaviour against a real broker. A chaos suite SHALL kill the node serving a link mid-flight, partition the network from it, and make the node that takes over serve the link, whether that is an HA backup or a replica that becomes leader, checking the contract for loss and duplicates.

#### Scenario: A driver loses a message on failover
- **WHEN** a driver claims at least once and loses a confirmed message when the serving node is killed
- **THEN** the chaos suite fails

#### Scenario: A replicated log fails over
- **WHEN** the chaos suite kills the leader serving a link on a system with replicated partitions
- **THEN** a follower takes over, and the suite checks the same loss and duplicate contract as for an HA pair
