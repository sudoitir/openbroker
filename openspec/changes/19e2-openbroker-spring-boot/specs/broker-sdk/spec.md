## MODIFIED Requirements

### Requirement: The SDK is a framework-free library with a driver SPI
The SDK SHALL ship as `openbroker-api` (the consumer API and the driver SPI in separate packages, with a `module-info.java`), `openbroker-support`, `openbroker-tck`, `openbroker-bom` and `openbroker-parent`, targeting Java 25, from its own public repository.
- `openbroker-api` SHALL depend on nothing except nullness annotations.
- No driver, `openbroker-support` or `openbroker-tck` SHALL depend on a framework; Spring SHALL appear only in the optional Spring Boot modules.
- The SDK SHALL carry no product type or brand other than its own.

#### Scenario: An outside project adopts the SDK
- **WHEN** a project with no Spring dependency adds `openbroker-api` and a driver
- **THEN** it can connect to a cluster through `BrokerDrivers` and use every facet the driver offers, without importing the SPI

#### Scenario: Spring leaks into a driver
- **WHEN** a change adds a Spring dependency to a driver or to `openbroker-api`
- **THEN** the build fails its dependency check

### Requirement: The SDK is versioned and released on its own
- The SDK SHALL follow Semantic Versioning, `0.x` until the first stable release, and SHALL be released to Maven Central from a version tag.
- A binary or source break in the API, outside types marked incubating, SHALL require a new minor version while in `0.x`.
- A consumer's release SHALL never depend on an unreleased SDK version.
- Optional modules (`openbroker-opentelemetry`, `openbroker-spring-boot-autoconfigure`, `openbroker-spring-boot-starter`) SHALL be versioned and released with the SDK and listed in the BOM; the benchmark module SHALL NOT be published.

#### Scenario: A breaking change in the API
- **WHEN** a pull request removes a public method of `openbroker-api` without raising the minor version
- **THEN** the API compatibility check fails

#### Scenario: The transport leaves incubation
- **WHEN** the transport types change shape and lose the incubating mark
- **THEN** the release raises the minor version

#### Scenario: The starter is aligned
- **WHEN** an application imports the BOM and adds the starter and a driver without versions
- **THEN** all three resolve to the same SDK release
