## Purpose

Defines the Apache ActiveMQ Artemis driver: how it maps Artemis onto the SDK's vocabulary, which channels it uses, and what it offers beyond the shared API.

## Requirements

### Requirement: Artemis is a driver of the SDK
- All Artemis-specific transport code (Jolokia, the Core protocol client, MBean names, notifications, relay and broker.xml remedy snippets) SHALL live in `openbroker-artemis`.
- No consumer module SHALL import the Artemis client or Jolokia types; build-time architecture tests SHALL enforce this.
- Artemis-only management SHALL be reached through `unwrap(ArtemisManagement.class)`.

#### Scenario: A feature needs an Artemis-only function
- **WHEN** a consumer needs broker.xml settings, reset counters or message expiry
- **THEN** it reaches them through `unwrap(ArtemisManagement.class)`, and the neutral facets do not offer them

### Requirement: Moving behind the SDK changes no behaviour
For every operation a consumer previously performed on Artemis directly, the requests sent through the driver SHALL match the transcripts recorded before the move:
- the same bodies, normalized headers and number of requests per node;
- the same permits and costs per node;
- the same Core session and consumer settings.

This covers every integration test of the consumer, not a sample.

#### Scenario: Scrape load is unchanged
- **WHEN** a scrape cycle runs against a node
- **THEN** it sends the same requests, with the same permits, as before

#### Scenario: The relay keeps its guarantees
- **WHEN** a transfer is killed mid-batch, or between its two commits
- **THEN** no message is lost or duplicated, as before

### Requirement: The Artemis driver passes the TCK on the supported range
The Artemis driver SHALL pass every TCK case for the capabilities it declares, against the oldest and the newest supported Artemis versions.

#### Scenario: CI runs the TCK
- **WHEN** the driver's integration tests run
- **THEN** every declared capability's TCK cases pass on both versions

### Requirement: Artemis filters cannot be escaped
The driver SHALL parse every Core filter with Artemis's selector grammar, and SHALL refuse a filter that is not one complete expression before composing it with any bound.

#### Scenario: A crafted filter
- **WHEN** a selection by filter carries `a=1) OR (1=1` and a production-time bound
- **THEN** it is refused with `INVALID_FILTER`
