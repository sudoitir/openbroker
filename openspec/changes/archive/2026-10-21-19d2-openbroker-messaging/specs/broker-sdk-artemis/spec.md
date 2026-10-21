## MODIFIED Requirements

### Requirement: Moving behind the SDK changes no behaviour
For every management and operator operation a consumer previously performed on Artemis directly, the requests sent through the driver SHALL match the transcripts recorded before the move:
- the same bodies, normalized headers and number of requests per node;
- the same permits and costs per node;
- the same Core session and consumer settings for operator calls (browse and send).

This covers every integration test of the consumer, not a sample. The transport's Core settings follow the `broker-sdk-messaging` capability instead.

#### Scenario: Scrape load is unchanged
- **WHEN** a scrape cycle runs against a node
- **THEN** it sends the same requests, with the same permits, as before

#### Scenario: The relay keeps its guarantees
- **WHEN** a transfer is killed mid-batch, or between its two commits
- **THEN** no message is lost or duplicated, as before

### Requirement: The Artemis driver passes the TCK on the supported range
The Artemis driver SHALL pass every TCK case for the capabilities it declares, including the messaging and chaos suites, against the oldest and the newest supported Artemis versions. The chaos suite SHALL run against a broker behind a network proxy and a shared-store primary and backup pair.

#### Scenario: CI runs the TCK
- **WHEN** the driver's integration tests run
- **THEN** every declared capability's TCK cases pass on both versions

#### Scenario: CI runs the chaos suite
- **WHEN** the chaos job runs on both versions
- **THEN** the broker kill, partition and failover cases pass

## ADDED Requirements

### Requirement: The Artemis driver implements the messaging client
The driver SHALL declare at most once, at least once and transacted; deduplication through `_AMQ_DUPL_ID` with the address's id cache as the window; group and subscription ordering; byte-bounded prefetch; release settlement across the receiver for a pull receiver; joined transactions; failover within an HA group; and client enforcement of managed retries. It SHALL map:
- a delayed release to a scheduled copy on the same queue, confirmed before the original is acknowledged;
- a rejection to a copy on the dead-letter destination carrying the original address, queue and id, confirmed before the original is acknowledged, or to an acknowledgement where the address has no dead-letter address;
- the system retry policy to a counted rollback under the address settings.
It SHALL refuse a managed policy whose attempts exceed the address's maximum delivery attempts with `FAILED_PRECONDITION` and a broker.xml remedy.

#### Scenario: A rejection dead-letters at once
- **WHEN** a consumer rejects a message on a queue whose address has a dead-letter address
- **THEN** the message is in the dead-letter queue with its original address and queue, and is not delivered again

#### Scenario: A partition is declared unimplemented
- **WHEN** a consumer asks for partition ordering
- **THEN** the call fails with `UNIMPLEMENTED`

### Requirement: Artemis reports an address's delivery modes
The driver SHALL read an address's routing types in every shape the broker returns them (a JSON array in a string, or an array), SHALL expose them as the comma-separated raw attribute, and SHALL report anycast as point to point and multicast as publish-subscribe.

#### Scenario: A listed address
- **WHEN** a consumer lists addresses on a real broker
- **THEN** an anycast address reports point-to-point delivery
