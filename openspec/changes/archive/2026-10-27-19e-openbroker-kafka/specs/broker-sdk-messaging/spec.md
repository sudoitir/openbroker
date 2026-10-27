## MODIFIED Requirements

### Requirement: Sends can be idempotent
A sender SHALL accept a deduplication id per message. The transport SHALL declare the strongest deduplication scope it offers: none, the sender's own session, or caller ids within a window, reporting the window per destination. A sender SHALL ask for a minimum scope, none by default. A sender that asks for any scope SHALL give a message without an id one of its own, so that its own re-sends are deduplicated. A sender that asks for a wider scope than the transport declares SHALL be refused with `UNIMPLEMENTED`, naming the scope offered, before anything is sent. Ordered at-least-once sending SHALL require at least the session scope.

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
A processor SHALL take a retry policy. The default policy SHALL resolve at open, against the subscription kind's declaration: to the system policy where the system counts attempts, and otherwise to a managed policy of ten attempts with the default backoff, retried in place. Default options thus open on every driver. The processor SHALL report the policy it resolved to. The system policy SHALL hand a failed message back counted and leave delay, attempts and dead-lettering to the system. A managed policy SHALL count attempts and wait the backoff between them: in place when processing is ordered or the policy is the resolved default, by a delayed release otherwise. After the last attempt it SHALL reject the message to the policy's dead-letter destination or the system's. A rejection SHALL dead-letter at once. For each subscription kind, the transport SHALL declare which of delay, attempt counting and dead-lettering the system itself enforces. A driver SHALL refuse with `UNIMPLEMENTED` an explicit system policy where the system counts no attempts, and with `FAILED_PRECONDITION` a managed policy that the system's own limits would cut short.

#### Scenario: A message that keeps failing
- **WHEN** a handler fails every time under a managed policy of three attempts with a backoff
- **THEN** it is called three times, at least the backoff apart, and the message lands in the dead-letter destination with the reason

#### Scenario: Ordered retries keep the order
- **WHEN** an ordered processor's handler fails twice on a group's first message and then succeeds
- **THEN** the group's later messages are handled after it, in order

#### Scenario: The default policy on a system without retries
- **WHEN** a processor opens with default options on a subscription whose system enforces no retries
- **THEN** it opens with a managed policy of ten attempts, reports that policy, and a failing message is retried in place and dead-lettered by the client

#### Scenario: The default policy on a system that retries
- **WHEN** a processor opens with default options on a subscription whose system counts attempts
- **THEN** it opens with the system policy and reports it

#### Scenario: A system policy the system does not keep
- **WHEN** a processor opens with an explicit system policy on a subscription whose system counts no attempts
- **THEN** it is refused with `UNIMPLEMENTED`, and no group is joined

### Requirement: The TCK proves every guarantee, including under failure
The TCK SHALL test each declared guarantee, idempotence scope, ordering scope, flow-control bound, retry policy, joined transaction and security behaviour against a real broker. A chaos suite SHALL kill the node serving a link mid-flight, partition the network from it, and make the node that takes over serve the link, whether that is an HA backup or a replica that becomes leader, checking the contract for loss and duplicates.

#### Scenario: A driver loses a message on failover
- **WHEN** a driver claims at least once and loses a confirmed message when the serving node is killed
- **THEN** the chaos suite fails

#### Scenario: A replicated log fails over
- **WHEN** the chaos suite kills the leader serving a link on a system with replicated partitions
- **THEN** a follower takes over, and the suite checks the same loss and duplicate contract as for an HA pair

## ADDED Requirements

### Requirement: Capabilities are declared per subscription kind
A transport SHALL declare, for each subscription kind it offers, the orderings, the pull settlement scope, whether transactions can be joined, and what the system enforces for retries. Asking for the capabilities of a kind the transport does not offer SHALL fail with `INVALID_ARGUMENT`. A declaration that depends on the cluster's features SHALL be computed at connect and SHALL be re-checked when a processor opens.

#### Scenario: Two subscription kinds on one system
- **WHEN** a consumer reads the capabilities of a transport that offers an ordered subscription kind and an unordered queue kind
- **THEN** each kind reports its own orderings, settlement scope, joined transactions and retry enforcement

### Requirement: Long handlers keep their message
Where the system locks a delivered message for a limited time, the processor SHALL renew the lock while a handler is still running, so that a slow handler does not lose its message to another consumer. It SHALL do this without any API change.

#### Scenario: A handler outlasts the lock
- **WHEN** a handler runs for twice the system's lock duration and then accepts its message
- **THEN** the message is not delivered to another consumer, and the accept succeeds
