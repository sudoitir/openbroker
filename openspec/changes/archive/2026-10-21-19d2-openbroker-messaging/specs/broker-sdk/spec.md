## MODIFIED Requirements

### Requirement: Transport separates settlement from transactions
Receivers SHALL consume or browse. They SHALL settle messages by accepting, releasing (with an optional delay and a delivery-failed flag) or rejecting (with an optional reason). Transacted batches SHALL be a capability a driver declares, not an assumption, and a commit whose answer never arrived SHALL be reported as in doubt. The transport's capabilities SHALL be static and SHALL be read from the transport itself. The transport API SHALL be versioned like the rest of the API, not marked incubating, and its guarantees SHALL follow the `broker-sdk-messaging` capability.

#### Scenario: A relay on a system without transactions
- **WHEN** a consumer asks a driver without the transacted capability for a transacted sender
- **THEN** the call fails with `UNIMPLEMENTED`, and the transport's capabilities had already said so

#### Scenario: The transport is not incubating
- **WHEN** the API compatibility check runs
- **THEN** it covers `Transport` and every messaging type

### Requirement: The SDK is versioned and released on its own
- The SDK SHALL follow Semantic Versioning, `0.x` until the first stable release, and SHALL be released to Maven Central from a version tag.
- A binary or source break in the API, outside types marked incubating, SHALL require a new minor version while in `0.x`.
- A consumer's release SHALL never depend on an unreleased SDK version.
- Optional modules (`openbroker-opentelemetry`) SHALL be versioned and released with the SDK and listed in the BOM; the benchmark module SHALL NOT be published.

#### Scenario: A breaking change in the API
- **WHEN** a pull request removes a public method of `openbroker-api` without raising the minor version
- **THEN** the API compatibility check fails

#### Scenario: The transport leaves incubation
- **WHEN** the transport types change shape and lose the incubating mark
- **THEN** the release raises the minor version
