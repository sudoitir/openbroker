Rules for every unit:
- Wrap each heavy command in `systemd-run --user --scope -q -p MemoryMax=3G -p MemorySwapMax=2G -- …`, with at most two at once.
- Read narrowly: `grep -n` first, then only the lines needed.
- Format with `./mvnw process-sources`.
- Commits follow `.claude/rules/05-commits.md`, with `!` and a `BREAKING CHANGE:` footer on API commits.

Order and parallelism:
- **Unit 1 runs first, alone.**
- **Units 2, 3, 4 and 5 are file-disjoint and run in parallel after unit 1.**
- **Unit 6 runs after unit 2.** Its ITs also need unit 4.
- **Unit 7 runs after units 3 and 6.**
- **Unit 8 runs last.**

## 1. API and build skeleton (unit A, first; files: openbroker-api/**, root and module poms, BOM)
- [x] 1.1 Raise every pom to `0.2.0-SNAPSHOT`; keep `openbroker.previous.version` at `0.1.0`
- [x] 1.2 Add modules `openbroker-opentelemetry` and `openbroker-benchmarks` (skeleton poms; benchmarks: deploy and publishing skipped, not in the BOM, `japicmp.skip`); add `openbroker-opentelemetry` to the BOM; manage `opentelemetry-bom`, `jmh`, `artemis-server` and `testcontainers-toxiproxy` versions (look the latest up with ctx7)
- [x] 1.3 Add the enums `Guarantee`, `OrderingScope`, `LinkState`, `PrefetchBound`, `SettlementScope`, `Enforcement`, `RetryMode`, `OperationType`
- [x] 1.4 Add the records `Backoff`, `Recovery`, `SendBuffer`, `FlowControl`, `RetryPolicy`, `SenderOptions`, `ReceiverOptions` (replacing `ReceiveOptions`), `ProcessorOptions`, `DeduplicationWindow`, `RetrySupport`, `TransportCapabilities`, `LinkEvent`, `OperationInfo`, each with defaults and validation
- [x] 1.5 Change `SendReceipt` (add `resent`); add `Offer`; reshape `Transport`, `Sender` and `Receiver`; add `Processor`, `MessageHandler`, `MessageContext`, `LinkListener`, `MessagingObserver`, `Observation` (with `none()`)
- [x] 1.6 Remove `@Incubating` from every messaging type and from `ClusterConnection.transport()`; remove `Capability.TRANSPORT_TRANSACTED`
- [x] 1.7 `ConnectOptions`: add `observer` and `withObserver`; `ErrorReasons`: add `SEND_BUFFER_FULL`, `DELIVERY_STALE`, `TRANSACTION_ROLLED_BACK`, `NOT_SUPPORTED`, `RESOURCE`
- [x] 1.8 Unit tests for record validation, `Backoff.delay` and the defaults
- [x] 1.9 Keep the Artemis driver compiling with `UNIMPLEMENTED` stubs for the new methods, so `main` builds between units
- Check: `./mvnw -B -pl openbroker-api,openbroker-support,openbroker-tck,openbroker-artemis -am -DskipITs test`

## 2. Support messaging engine (unit B, parallel; files: openbroker-support/src/**/support/messaging/**)
- [ ] 2.1 `Backoffs` (jitter on an injected `Clock` and random source)
- [ ] 2.2 `LinkSupervisor`: states, serial events per link, recovery loop with backoff and give-up, `link.recover` admitted by the interceptor, `ReentrantLock` only
- [ ] 2.3 `SendPipeline`: window by count and bytes, `offer`/`send`/`flush`, one writer virtual thread per link, confirmation tracking, ordered resend on recovery, completion on a virtual-thread executor wrapped by `contextPropagation`, observer calls
- [ ] 2.4 `ProcessorEngine`: lanes over a driver `LaneFactory`, GROUP/PARTITION dispatch by hashing when the system does not pin groups, the retry policy (in place when ordered, delayed release when unordered, dead-letter), stop with grace, observer scope around the handler
- [ ] 2.5 Unit tests against a fake lane and writer, including the bounds, retry timings, ordering and the pinning JFR check
- Check: `./mvnw -B -pl openbroker-support -am test`

## 3. OpenTelemetry module (unit C, parallel; files: openbroker-opentelemetry/**)
- [x] 3.1 `OpenTelemetryMessaging.of(OpenTelemetry)`: spans per the mapping table, links from extracted context, W3C injection through the configured propagator
- [x] 3.2 The four semconv metrics plus `openbroker.messaging.link.recoveries`, with `error.type` = the error reason
- [x] 3.3 `module-info` and tests with `opentelemetry-sdk-testing` (span names, kinds, attributes, process→send link)
- Check: `./mvnw -B -pl openbroker-opentelemetry -am test`

## 4. TCK (unit D, parallel; files: openbroker-tck/**)
- [ ] 4.1 `MessagingTck` cases 1–21 (design §TCK), skipping what the capabilities declare unavailable; `TckEnvironment.restrictedRequest()` and `mutualTlsRequest()`
- [ ] 4.2 `TckChaos` and `ChaosTck` cases 1–12
- [ ] 4.3 Port the existing transport cases in `OpenBrokerTck` to the new API
- [ ] 4.4 Case `listedDestinationReportsDeliveryModes` in `OpenBrokerTck` (the regression guard for commit 06aa372)
- Check: `./mvnw -B -pl openbroker-tck -am compile`

## 5. ADRs and docs (unit G, parallel; files: docs/adr/0010–0017, README.md, package-info.java of messaging)
- [ ] 5.1 Write ADRs 0010–0017 from the design (Nygard, `000-template.md`); index them in README "Decisions"
- [ ] 5.2 README: a messaging quick start (sender, offer, processor, retry policy, observer) and the loss/duplicate table; the modules table gains opentelemetry
- [ ] 5.3 Messaging `package-info.java`: the contract summary
- Check: `./mvnw -B -pl openbroker-api -am javadoc:javadoc -q`

## 6. Artemis driver (unit E, after 2; files: openbroker-artemis/src/main/**, its test resources and tests)
- [ ] 6.1 `CoreLinks`: link settings in the key (guarantee, window, preAcknowledge, confirmation window), connect outside `compute`, factories reference-counted per TLS id, `Timeouts` → failure check and TTL
- [ ] 6.2 Core sender over `SendPipeline`: async send acknowledgements, durable enforcement, `_AMQ_DUPL_ID`, `sendFailed` mapping, the dedupe window from address settings
- [ ] 6.3 Core receiver and lanes: guarantees, credit and window, per-message Accept, Release (rollback, or a delayed copy to the FQQN), Reject (copy to the DLA with the `_AMQ_ORIG_*` properties), DELIVERY_STALE, the joined sender
- [ ] 6.4 `process` over `ProcessorEngine`; MANAGED retry check against `max-delivery-attempts`, with a broker.xml remedy
- [ ] 6.5 Recovery: HA group from `Membership.group`, reopening on the serving member, events with `servedBy`
- [ ] 6.6 Map `TRANSACTION_ROLLED_BACK`, `NOT_SUPPORTED` and the resource metadata in `CoreLinks.failure` and `Unimplemented`
- [ ] 6.7 `capabilities()` as in the design table
- [ ] 6.8 Unit tests: `CoreLinksSettingsTest` and `TransportsTest` updated, settlement mapping, copy properties
- [ ] 6.9 `ArtemisMessagingTckIT` (MessagingTck): a restricted account and a TLS acceptor with client auth, using throwaway test certificates under test resources plus a script that regenerates them
- [ ] 6.10 `ArtemisChaosIT` (ChaosTck): Toxiproxy, then the shared-store pair through an `etc-override` broker.xml on a shared volume. Prove it on `apache/activemq-artemis:2.33.0` first
- Check: `./mvnw -B -pl openbroker-artemis -am verify -Dit.test=ArtemisMessagingTckIT`, then `… -Pchaos -Dit.test=ArtemisChaosIT`

## 7. Benchmarks and CI (unit F, after 3 and 6; files: openbroker-benchmarks/**, .github/workflows/ci.yml)
- [ ] 7.1 Benchmarks `CoreCodecBench`, `SendBench`, `ProcessBench`, `RoundTripBench`, `ObserverBench` on an embedded in-VM Artemis
- [ ] 7.2 `Budgets` main: `Runner` with `GCProfiler`, compared with `budgets.json`, exits non-zero on a breach
- [ ] 7.3 CI: a `chaos` job (matrix of both images, `-Pchaos`, 15-minute timeout) and a `bench` job; both in `ci-ok`; exclude `*ChaosIT` from `verify`
- [ ] 7.4 Record the baselines from the first CI run into `budgets.json` (allocation +10%/64 B, throughput floor 50%, p99 ceiling 2×)
- Check: `./mvnw -B -pl openbroker-benchmarks -am package -DskipTests && java -jar openbroker-benchmarks/target/benchmarks.jar`

## 8. Verification and release
- [ ] 8.1 `./mvnw -B verify` with `-Dartemis.image=apache/activemq-artemis:2.33.0` and with `apache/artemis:2.57.0` (in CI)
- [ ] 8.2 The chaos job is green on both images; the bench job is within budget
- [ ] 8.3 `./mvnw -B -Papi-check verify -DskipTests -DskipITs`: passes only because the version is 0.2.0 (expected breaks: every messaging type, `SendReceipt`, `ConnectOptions`, `Capability`, `ClusterConnection`)
- [ ] 8.4 The API commit is marked `!` with a `BREAKING CHANGE:` footer naming the transport reshape and the removed `Capability.TRANSPORT_TRANSACTED` (the generated release notes carry it)
- [ ] 8.5 Open the PR; merge on green CI; `/opsx:archive`; a maintainer runs the `Release` workflow with `0.2.0`; a follow-up PR raises `openbroker.previous.version` to `0.2.0` and removes `japicmp.skip` from `openbroker-opentelemetry`
- [ ] 8.6 Follow-up proposal (not this change): a Spring Boot starter module binding the options records from `application.yml`
