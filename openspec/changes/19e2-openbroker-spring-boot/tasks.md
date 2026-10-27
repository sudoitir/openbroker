Rules:
- Wrap each heavy command in `systemd-run --user --scope -q -p MemoryMax=3G -p MemorySwapMax=2G -- …`, and run at most two at once.
- Read narrowly. Format with `./mvnw process-sources`. Commits follow `.claude/rules/05-commits.md`.
- Spring Boot, Actuator, SSL bundle and Micrometer facts are checked with ctx7 or the Boot 4.1.2 sources before use.

## 1. Build and SDK gaps
- [ ] 1.1 Raise the version to `0.4.0-SNAPSHOT` and `openbroker.previous.version` to `0.3.0`. Check: `./mvnw -B -q -DskipTests verify -pl openbroker-api`
- [ ] 1.2 Add `ConnectionSchema.validate` and wire it into `BrokerDrivers.connect` and `verify` (D2). Check: `openbroker-api` tests for an unknown key, a missing required key, a wrong type, a value that is not allowed, and a redacted sensitive value
- [ ] 1.3 Add `MessagingObserver.composite` (D6). Check: unit tests for fan-out and error isolation
- [ ] 1.4 Add `OpenTelemetryMessaging.builder(...).injectContext(boolean)` (D7). Check: a send is a span, and the message keeps the producer's `traceparent`

## 2. Modules and the framework-free guard
- [x] 2.1 Add `openbroker-spring-boot-autoconfigure` and `openbroker-spring-boot-starter` to the reactor and the BOM, with Spring Boot 4.1.2 dependency management only in the autoconfigure module (D12). Check: `./mvnw -B -DskipTests verify`
- [x] 2.2 Add the Enforcer rule that bans Spring outside the two modules (D11). Check: the build fails when the test profile adds Spring to `openbroker-api`

## 3. Binding
- [x] 3.1 Add `OpenBrokerProperties`, `RecordBinder`, `CredentialsProperties` and `ConnectionOptions` (D1). Check: `ApplicationContextRunner` tests bind a full and a partial YAML for each driver, reject an out-of-range value, and reject a dead-letter target
- [x] 3.2 Run schema validation at bind time, and add the `FailureAnalyzer`, with redacted messages. Check: misspelt-option, missing-driver and sensitive-value tests
- [x] 3.3 Add the configuration metadata, with descriptions. Check: a test that the metadata lists exactly the bindable keys

## 4. Building blocks and beans
- [x] 4.1 Add the `BrokerDrivers` bean, `OpenBrokerConnectOptions`, `ConnectOptionsCustomizer` and `SslBundleTls` (D4, D8). Check: context-runner tests for interceptor, clock, customizers and the client defaults, plus a missing-bundle test and a bundle-reload test
- [x] 4.2 Add per-connection `ClusterConnection` and `Transport` beans by name, with the primary rule and back-off (D3). Check: context-runner tests for one connection, two connections and an overridden connection, and a `CredentialSource` bean that wins

## 5. Spring Boot connection details
- [x] 5.1 Derive the `kafka` and `artemis` connections from Boot's details, with the overlay and the opt-out (D9). Check: context-runner tests for plaintext, SSL bundle, SCRAM, failing OAUTHBEARER, embedded Artemis skipped, overlay and opt-out

## 6. Observability and lifecycle
- [x] 6.1 Add the health composite with deadlines and redacted details (D5). Check: tests for up, partial, down, timeout, stale and closing
- [x] 6.2 Add the Micrometer observer, and the OpenTelemetry observer with the meter rule (D6). Check: meter and span assertions, a no-double-count test, and a test that nothing is wired when neither is present
- [x] 6.3 Add the `SmartLifecycle` and `OpenBrokerProcessors` (D10). Check: graceful-shutdown order and no-handler-before-ready tests

## 7. End to end and documentation
- [ ] 7.1 Add one Spring Boot application IT per driver, with Artemis and Kafka containers wired by `@ServiceConnection` and no `openbroker.*` keys: send, process, health and shutdown. Run the Kafka IT in CI's Kafka leg. Check: green in `verify`
- [ ] 7.2 Add the README section and ADRs 0026–0030, and the `CLAUDE.md` layout rows. Check: the README's YAML is the one a test binds
- [ ] 7.3 Run the full `./mvnw verify` once before the PR. Check: green locally and in CI

## Workflow follow-up
- PR, merge on green CI, `/opsx:archive`, then dispatch `Release` with `0.4.0` and confirm it on Central.
