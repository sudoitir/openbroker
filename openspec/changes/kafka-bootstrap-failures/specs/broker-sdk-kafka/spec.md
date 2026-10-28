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
