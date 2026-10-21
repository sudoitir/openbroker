Rules:
- Wrap each heavy command in `systemd-run --user --scope -q -p MemoryMax=3G -p MemorySwapMax=2G -- …`.
- Read narrowly. Format with `./mvnw process-sources`. Commits follow `.claude/rules/05-commits.md`.
- Spring Boot, Actuator, SSL bundle and Micrometer facts are checked with ctx7 before use.

## 1. Modules and the framework-free guard
- [ ] 1.1 Add `openbroker-spring-boot-autoconfigure` and `openbroker-spring-boot-starter` to the reactor and the BOM, with Spring Boot 4 dependency management only in the autoconfigure module. Check: `./mvnw -B -DskipTests verify`
- [ ] 1.2 The Enforcer rule that bans Spring outside the two modules. Check: the build fails when a test profile adds Spring to `openbroker-api`

## 2. Binding
- [ ] 2.1 `OpenBrokerProperties` binding `ClusterSpec`, credentials, `ConnectionOptions`, `SenderOptions`, `ReceiverOptions` and `ProcessorOptions`; fix in `openbroker-api` any record that does not bind. Check: `ApplicationContextRunner` tests bind a full YAML example per driver
- [ ] 2.2 Startup validation against the driver's `OptionSpec` schema, with redacted messages. Check: misspelt-option and missing-driver tests
- [ ] 2.3 Configuration metadata from the processor, with descriptions. Check: the generated `spring-configuration-metadata.json` lists every key

## 3. Beans
- [ ] 3.1 Per-connection `ClusterConnection` and `Transport` beans by name, the primary rule and back-off. Check: context-runner tests for one, two and overridden connections
- [ ] 3.2 TLS from SSL bundles, a `CredentialSource` bean, a `CallInterceptor`, a `Clock`, and Micrometer context propagation. Check: context-runner tests, and a bundle-reload test

## 4. Observability and lifecycle
- [ ] 4.1 The health composite with deadlines and redacted details. Check: up, down and timeout tests
- [ ] 4.2 The Micrometer `MeterBinder` and the OpenTelemetry observer when beans exist. Check: meter and span assertions in context-runner tests
- [ ] 4.3 The `SmartLifecycle` and the `OpenBrokerProcessors` helper. Check: a graceful-shutdown test

## 5. End to end and documentation
- [ ] 5.1 One Spring Boot application IT per driver (Artemis and Kafka containers): send, process, health and shutdown. Check: green in `verify`
- [ ] 5.2 README section and ADRs 0026 and 0027. Check: the README's YAML is the one the IT binds
- [ ] 5.3 Full `./mvnw verify` once before the PR. Check: green locally and in CI

## Workflow follow-up
- PR, merge on green CI, `/opsx:archive`, then ask the maintainer to release the next minor version.
