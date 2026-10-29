## ADDED Requirements

### Requirement: Unreachable bootstrap servers are classified
Before making a client for the bootstrap servers, the driver SHALL resolve each and open a connection to it, and SHALL give the client only those that answer. When none answers it SHALL fail at once with `UNAVAILABLE` and `UNREACHABLE`, not sent, naming the first server and its cause (`dns`, `refused` or `timeout`) in the error metadata and every server in the message.

#### Scenario: A name that does not resolve
- **WHEN** a consumer verifies a connection whose only bootstrap server does not resolve
- **THEN** verify fails at once as unavailable and not sent, with the cause `dns` and a message that the server does not resolve

#### Scenario: A closed port
- **WHEN** a consumer verifies a connection whose only bootstrap server refuses connections
- **THEN** verify fails at once, long before the deadline, with the cause `refused`

#### Scenario: One server answers
- **WHEN** a consumer connects with three bootstrap servers, one of which answers
- **THEN** the client is given that one server and the connection works

### Requirement: Hanging transactions are found and aborted on demand
The driver SHALL list, on demand and never on a watch, the transactions that hang: open on a partition for longer than the broker's `transaction.max.timeout.ms` and not listed by the coordinator as in progress, found with one producer read per broker for the partitions in scope. Each SHALL name its topic, partition, producer, epochs, start offset, the records it holds back and its age. Resolving one SHALL abort it, with a preview and an etag over its start offset and epoch, and SHALL fail with `FAILED_PRECONDITION` once it is no longer open.

#### Scenario: A transaction the coordinator still lists
- **WHEN** a transaction is open longer than the longest allowed but the coordinator lists it as ongoing
- **THEN** it is not listed as hanging, because the coordinator will abort it

#### Scenario: A transaction the coordinator does not know
- **WHEN** a producer has an open transaction on a partition older than the longest allowed, and the coordinator lists no transaction for it
- **THEN** it is listed with the records it holds back, and resolving it aborts it so that read-committed consumers move on

#### Scenario: Resolving one that has closed
- **WHEN** a hanging transaction is resolved after it was aborted
- **THEN** the resolution fails with `FAILED_PRECONDITION` and changes nothing
