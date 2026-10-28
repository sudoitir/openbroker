# OpenBroker

One Java SDK, and one vocabulary, for every message broker and queue service.

OpenBroker lets a backend inspect, operate and move messages on Apache ActiveMQ Artemis and
Apache Kafka today, and on RabbitMQ, ActiveMQ Classic and others as their drivers arrive, through one small,
framework-free API. Drivers plug in like JDBC drivers, and a conformance kit (the TCK) proves each
one against a real broker.

Status: pre-release (`0.x`). The API may change between minor versions until 1.0.

## Modules

| Artifact | What it is |
| --- | --- |
| `openbroker-api` | The API you program against, and the driver SPI. Depends on JSpecify only. |
| `openbroker-support` | Shared helpers for drivers and consumers: the portable filter dialect, polling notifications, counter rates. |
| `openbroker-tck` | Abstract JUnit contract tests a driver extends. |
| `openbroker-artemis` | The Apache ActiveMQ Artemis driver: management over Jolokia, messages and notifications over Core. |
| `openbroker-kafka` | The Apache Kafka driver (3.0 and later): management over Admin, messages over the producer, consumer and share consumer. |
| `openbroker-opentelemetry` | Optional: maps the messaging observer to OpenTelemetry spans, metrics and W3C trace context. |
| `openbroker-spring-boot-starter` | Optional: everything a Spring Boot 4 application needs; add the drivers it uses. |
| `openbroker-spring-boot-autoconfigure` | Optional: the auto-configuration behind the starter. |
| `openbroker-bom` | Aligns the versions above. |

Java 25. The modules ship `module-info` descriptors (the driver an `Automatic-Module-Name`).

## Quick start

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>io.github.sudoitir</groupId>
      <artifactId>openbroker-bom</artifactId>
      <version>0.4.0</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>
<dependencies>
  <dependency>
    <groupId>io.github.sudoitir</groupId>
    <artifactId>openbroker-api</artifactId>
  </dependency>
  <dependency>
    <groupId>io.github.sudoitir</groupId>
    <artifactId>openbroker-artemis</artifactId>
    <scope>runtime</scope>
  </dependency>
</dependencies>
```

```java
try (BrokerDrivers drivers = BrokerDrivers.load();
        ClusterConnection cluster = drivers.connect(new ConnectRequest(
                "artemis",
                ClusterSpec.ofNodes(List.of(new NodeSpec(
                        NodeId.of("primary"),
                        List.of(
                                Endpoint.of(Plane.MANAGEMENT, "jolokia", "http://broker:8161/console/jolokia"),
                                Endpoint.of(Plane.DATA, "core", "tcp://broker:61616"))))),
                CredentialSource.of(Credentials.usernamePassword("admin", System.getenv("BROKER_PASSWORD"))),
                ConnectOptions.defaults()))) {

    // Every subscription (an Artemis queue) with its backlog, on every node.
    Partial<ResourcePage> page = cluster.resources().read(ResourceQuery.of(ResourceClass.SUBSCRIPTION));
    for (Resource r : page.orThrow().items()) {
        long backlog = r.attributes().get(MetricKeys.BACKLOG).map(MetricValue::asLong).orElse(0L);
        System.out.println(r.id() + " " + backlog);
    }

    // Send one message to a destination (an Artemis address).
    cluster.messages()
            .send(
                    SendTarget.destination(ResourceId.of("artemis.address", "orders")),
                    OutboundMessage.of(MessageBody.of("hello".getBytes(StandardCharsets.UTF_8))));
}
```

- **Resources** are read by class (`DESTINATION`, `SUBSCRIPTION`, `CONSUMER`, …) or by a system's
  kind (`artemis.queue`), with standard attributes and metric keys (`openbroker.backlog`, …).
- **Operations** are previewed before they run: a preview states the change, an estimate and an
  etag; `execute(…, Precondition.ifMatch(etag))` refuses if the resource changed in between.
- **Results are honest.** A cluster-wide call returns a `Partial` naming each node's outcome. An
  error is a `BrokerException` with a gRPC status, a reason, and whether the request was sent.
- **What only one system has** is reached through `cluster.unwrap(…)`: for Artemis
  `ArtemisManagement`, for Kafka its `Admin` client. The connection owns the `Admin`; closing the view
  it hands out does nothing, and its calls bypass the `CallInterceptor`.

## Messaging

`cluster.transport()` is a messaging client with stated guarantees. A link declares its guarantee and
the driver refuses what it cannot keep (`UNIMPLEMENTED`). Sending is bounded, receiving is
credit-based, and a lost link is recovered by the driver under the rules below.

```java
Transport transport = cluster.transport().orElseThrow();
ResourceId orders = ResourceId.of("artemis.address", "orders");
ResourceId deadLetters = ResourceId.of("artemis.address", "orders.dead");

// At-least-once with deduplication: resends after a failure are deduplicated, in order.
SenderOptions sending = SenderOptions.defaults().withDeduplication(DeduplicationScope.SESSION).withOrdering(OrderingScope.GROUP);
try (Sender sender = transport.sender(SendTarget.destination(orders), Scope.whole(), sending)) {
    OutboundMessage message = OutboundMessage.of(MessageBody.of(payload))
            .withHeaders(MessageHeaders.builder().groupId("customer-7").build());
    // offer never blocks; Full says the bounded send buffer has no room.
    switch (sender.offer(message)) {
        case Offer.Accepted accepted -> accepted.receipt().thenAccept(receipt -> log(receipt.resent()));
        case Offer.Full full -> slowDown(full.inFlight(), full.bufferedBytes());
    }
    sender.send(message); // waits for room (up to maxBlock) and for the confirmation
    sender.flush(Duration.ofSeconds(10));
}

// Eight lanes on virtual threads; one group runs in order; the client retries up to 5 times.
ProcessorOptions processing = ProcessorOptions.defaults()
        .withConcurrency(8)
        .withOrdering(OrderingScope.GROUP)
        .withRetry(RetryPolicy.managed(5, Backoff.defaults()).withDeadLetter(SendTarget.destination(deadLetters)));
try (Processor processor = transport.process(
        ResourceId.of("artemis.queue", "orders"), Scope.whole(), processing, context -> {
            handle(context.message());
            return Disposition.accept(); // an exception or Release counts as a failed attempt
        })) {
    awaitShutdown();
}
```

Telemetry is a hook. Add `openbroker-opentelemetry` and pass the observer when connecting:

```java
ConnectOptions options = ConnectOptions.defaults().withObserver(OpenTelemetryMessaging.of(openTelemetry));
```

### What is lost or duplicated

| Guarantee | Link lost | Broker restart, no HA | Failover to a backup | Client dies | Answer lost |
| --- | --- | --- | --- | --- | --- |
| `AT_MOST_ONCE` send | unconfirmed may be lost, never resent (`UNAVAILABLE`, `IN_DOUBT`) | non-durable lost | unconfirmed may be lost | buffered lost | n/a |
| `AT_MOST_ONCE` receive | prefetched and handed-out lost | same | same | same | n/a |
| `AT_LEAST_ONCE` send | resent in order: no loss, duplicates possible (`resent=true`) | confirmed survive; unconfirmed resent | same | unconfirmed in the buffer lost | resent: duplicate possible |
| `AT_LEAST_ONCE` send, deduplicating | resent and deduplicated: no loss, no duplicate, order kept | same, within the dedupe window | same | as above | resent and deduplicated |
| `AT_LEAST_ONCE` receive, process | unsettled redelivered; a stale settle fails `DELIVERY_STALE` | redelivered | redelivered | redelivered | redelivered: duplicate |
| `TRANSACTED`, joined included | rolled back; commit fails `ABORTED`; redo | rolled back | rolled back | rolled back | `IN_DOUBT`; redo with the same dedupe ids |

After `giveUpAfter` a link is `FAILED`: pending receipts fail `UNAVAILABLE`, `IN_DOUBT`, and nothing is
resent. The dedupe window is reported by `Sender.deduplication()`.

## Kafka

`openbroker-kafka` supports Apache Kafka 3.0 and later, in ZooKeeper and KRaft mode, and refuses an older
cluster when it connects. It uses `kafka-clients` alone. What a cluster lacks is declared per feature, with
its reason, and never emulated:

| Capability | Needs | Otherwise |
| --- | --- | --- |
| `kafka.quorum` | KRaft mode | The quorum is not described; node health carries no voter state |
| `kafka.consumer-protocol-groups` | Kafka 4.0 with `group.version` 1 | Processors join with Kafka's classic protocol and its cooperative assignor |
| `kafka.share-groups` | Kafka 4.2 with `share.version` 1 | The share-group kind is not declared |
| `kafka.streams-groups` | `streams.version` 1 | Streams groups are not listed |
| `kafka.fenced-brokers` | KRaft and Kafka 4.0 | A fenced broker is missing from the nodes instead of shown as fenced |
| `kafka.eligible-leaders` | `eligible.leader.replicas.version` 1 | Partitions carry no `kafka.elr` attributes |

`verify` checks every broker on the address it advertises, in parallel and within the connect timeout, and
names each unreached broker and why (the name does not resolve, the connection is refused or times out, TLS
or authentication fails). On a ZooKeeper cluster before Kafka 3.3 with a broker down, the check that tells
the release fans out to every broker and fails, so `verify` fails as unreachable instead of naming the
broker.

- **Resources.** Brokers are nodes; topics, partitions, consumer groups, share groups, ACLs and
  transactional ids are kinds. Streams groups, and classic groups run by Kafka Streams, are listed
  read-only, because their offsets belong to the applications that run them. Changes are watched by polling, every `watch.interval`
  (ten seconds by default), including consumers joining and leaving a subscription.
- **Lag and counts are estimates.** Offset arithmetic overcounts on compacted and transactional
  topics, so every total says so.
- **Destructive operations are honest.** Purge and seek reset a group's committed offsets and fail
  `FAILED_PRECONDITION` while the group has members or is rebalancing, without removing them; an empty
  classic group can be rewound too. Truncate deletes records
  before an offset, with a preview, and is refused on a compacted topic. Moving, copying or deleting
  one message is unavailable, because a log cannot remove one record.
- **Hanging transactions** (`kafka.hanging-transaction`) are transactions the coordinator no longer
  tracks, open on a partition for longer than `transaction.max.timeout.ms`, which hold its last stable
  offset back so that `read_committed` consumers stall (KIP-664). They are listed on demand, never on the
  watch, for the cluster or one topic, and `RESOLVE` aborts one, previewing the records it holds back.
  A transaction the coordinator still lists heals itself and is not listed.
- **Browsing** reads committed records without joining a group.
- **Messaging.** Senders deduplicate within the producer session (`DeduplicationScope.SESSION`).
  Processors on a consumer or classic group settle by committing offsets, keep per-key order in lanes, and join
  transactions (KIP-447). A retry that is ordered, transacted or the default happens in place; an
  explicitly managed, unordered policy uses one retry topic per backoff step (`<topic>.retry-<n>`).
  Exhausted records go to `<topic>.dlt`. The processor refuses to open when a retry or dead-letter
  topic is missing and the account cannot create it.
- **Share groups** (Kafka 4.2 and later) are their own subscription kind: settlement per message, no
  ordering, at least once only, and the processor renews a record's lock while a slow handler runs.
  The broker counts attempts, so the default policy is the system's. From Kafka 4.4 with `share.version`
  2, the broker dead-letters to the group's configured topic; at `share.version` 1 a record past the
  delivery limit is archived with no copy, so choose a managed policy when you need the dead letter.
- **Security.** SASL (PLAIN, SCRAM, OAUTHBEARER with a refreshed bearer token), TLS and mutual TLS
  (`Credentials.None`). Hostname verification stays on.

## Spring Boot

`openbroker-spring-boot-starter` gives a Spring Boot 4 application its OpenBroker clients as beans. It is the
only place Spring appears: the API and the drivers stay framework-free, and the build enforces it.

```xml
<dependency>
  <groupId>io.github.sudoitir</groupId>
  <artifactId>openbroker-spring-boot-starter</artifactId>
</dependency>
<dependency>
  <groupId>io.github.sudoitir</groupId>
  <artifactId>openbroker-kafka</artifactId>
</dependency>
```

- **Nothing to configure** when Spring Boot already knows the broker. With a Kafka or Artemis driver on the
  class path, `spring.kafka.*`, `spring.artemis.*`, `@ServiceConnection` or Docker Compose give a
  connection named `kafka` or `artemis`, with its endpoints, security protocol, SSL bundle and (for Kafka
  `PLAIN` and `SCRAM-*`) the login from `sasl.jaas.config`. Spring states no Artemis management endpoint,
  so its management facets stay unavailable, and its health is `UNKNOWN`, until you add one. An
  `openbroker.connections.<name>` of the same name is laid over the derived connection key by key (nodes
  merge by id, other lists replace), and `openbroker.spring-connections.enabled=false` turns derivation off.
- **Connections in `application.yml`.** Each `openbroker.connections.<name>` binds the SDK's own records
  (`SenderOptions`, `ProcessorOptions` and the rest) over their own defaults, so setting one key keeps every
  other default. Credentials name their form. TLS comes from a Spring SSL bundle (`options.ssl-bundle`),
  read again on every connect, so a reloaded bundle reaches the next connection.

```yaml
openbroker:
  client:
    connect-timeout: 5s
  connections:
    orders:
      system: kafka
      cluster:
        cluster-endpoints:
          - { plane: data, protocol: kafka, address: "broker-1:9093" }
          - { plane: data, protocol: kafka, address: "broker-2:9093" }
        options:
          security.protocol: SASL_SSL
          sasl.mechanism: SCRAM-SHA-512
      credentials:
        form: username-password
        username: orders-app
        password: ${ORDERS_PASSWORD}
      options:
        ssl-bundle: kafka
      sender:
        guarantee: at-least-once
      processor:
        concurrency: 4
    billing:
      system: artemis
      cluster:
        nodes:
          - id: primary
            endpoints:
              - { plane: management, protocol: jolokia, address: "http://artemis:8161/console/jolokia" }
              - { plane: data, protocol: core, address: "tcp://artemis:61616" }
      credentials:
        form: username-password
        username: billing-app
        password: ${BILLING_PASSWORD}
```

- **Beans by name.** Each connection is a `ClusterConnection` and a `Transport` bean (`ordersClusterConnection`,
  `ordersTransport`), qualified by its name and primary when it is the only one. Your own bean of the same name
  wins. A `CredentialSource` bean qualified with the connection's name wins over its credentials. A wrong
  connection (a misspelt option, a value out of range, a missing driver, a dead-letter target in YAML) fails the
  start naming the connection and the key, never a value.
- **Runtime connections.** An application that opens connections from its own store gets the same pieces:
  the `BrokerDrivers` bean, `OpenBrokerConnectOptions.forConnection(name, system, tls)` (your interceptor,
  clock, observers and `ConnectOptionsCustomizer`s over the `openbroker.client.*` defaults) and
  `SslBundleTls.forBundle(...)`. Health and the processor lifecycle are not applied to them.
- **Health, metrics, tracing.** With Actuator, `openbroker` health has one indicator per connection:
  up while the cluster describes its nodes (a partial answer counts), down with the reason otherwise, with a
  deadline (`openbroker.health.timeout`) and at most one check at a time; the first check ever is `UNKNOWN`,
  not down, if it passes the deadline. Metrics go to Micrometer when a
  `MeterRegistry` is present and spans to OpenTelemetry when an `OpenTelemetry` bean is; with both, nothing is
  counted twice. Metrics are tagged with the connection, system and operation, not the destination (too many
  distinct values); spans carry it. `openbroker.tracing.inject-context=false` stops sends writing trace context into the message,
  for tools that relay messages other producers wrote.
- **Lifecycle.** Register processors with `OpenBrokerProcessors`: they start when the application is ready (after its runners) and
  stop within their `stopGrace` before any connection closes; the drivers close last.

## Writing a driver

Implement `io.github.sudoitir.openbroker.spi.BrokerDriver`, register it in
`META-INF/services/io.github.sudoitir.openbroker.spi.BrokerDriver`, and extend the TCK's contract
tests for the capabilities you declare. `openbroker-artemis` is the reference.

## Decisions

The design is recorded in [`docs/adr/`](docs/adr/).

- [0001](docs/adr/0001-one-sdk-and-one-vocabulary-for-every-broker.md) One SDK and one vocabulary for every broker
- [0002](docs/adr/0002-resources-have-a-class-and-a-system-kind.md) Resources have a class and a system kind
- [0003](docs/adr/0003-brokerdrivers-is-the-entry-point-and-drivers-are-found-by-serviceloader.md) `BrokerDrivers` is the entry point and drivers are found by `ServiceLoader`
- [0004](docs/adr/0004-errors-carry-a-status-a-reason-and-whether-the-request-was-sent.md) Errors carry a status, a reason and whether the request was sent
- [0005](docs/adr/0005-operations-are-previewed-and-run-against-an-etag.md) Operations are previewed and run against an etag
- [0006](docs/adr/0006-one-message-model-for-every-system.md) One message model for every system
- [0007](docs/adr/0007-standard-metric-keys.md) Standard metric keys
- [0008](docs/adr/0008-semver-0x-with-japicmp-and-incubating-types.md) SemVer `0.x` with japicmp and incubating types
- [0009](docs/adr/0009-a-maintainer-runs-each-release.md) A maintainer runs each release
- [0010](docs/adr/0010-the-transport-leaves-incubation-with-declared-guarantees.md) The transport leaves incubation with declared guarantees
- [0011](docs/adr/0011-drivers-own-recovery-and-failover.md) Drivers own recovery and failover
- [0012](docs/adr/0012-sending-is-bounded-and-backpressure-is-a-value.md) Sending is bounded and backpressure is a value
- [0013](docs/adr/0013-push-processing-runs-in-lanes-on-virtual-threads.md) Push processing runs in lanes on virtual threads
- [0014](docs/adr/0014-retries-default-to-the-systems-policy-managed-retries-run-in-the-client.md) Retries default to the system's policy; managed retries run in the client
- [0015](docs/adr/0015-telemetry-goes-through-a-hook-with-opentelemetry-in-an-optional-module.md) Telemetry goes through a hook, with OpenTelemetry in an optional module
- [0016](docs/adr/0016-performance-budgets-are-enforced-in-ci.md) Performance budgets are enforced in CI
- [0017](docs/adr/0017-chaos-is-part-of-the-tck.md) Chaos is part of the TCK
- [0031](docs/adr/0031-kafka-3-0-and-later-declared-per-feature.md) Kafka 3.0 and later, declared per feature
- [0019](docs/adr/0019-kafka-settles-by-offset-and-runs-keys-in-lanes.md) Kafka settles by offset and runs keys in lanes
- [0018](docs/adr/0018-the-kafka-mapping-and-its-honest-destructive-operations.md) The Kafka mapping and its honest destructive operations
- [0020](docs/adr/0020-deduplication-is-declared-as-a-scope.md) Deduplication is declared as a scope
- [0021](docs/adr/0021-the-retry-default-follows-what-the-system-enforces.md) The retry default follows what the system enforces, per subscription kind
- [0022](docs/adr/0022-share-groups-are-a-subscription-kind.md) Share groups are a subscription kind
- [0023](docs/adr/0023-credentials-are-a-sealed-set-of-forms.md) Credentials are a sealed set of forms
- [0024](docs/adr/0024-kinds-carry-a-label.md) Kinds carry a label
- [0025](docs/adr/0025-the-chaos-fixture-names-the-serving-node-and-its-successor.md) The chaos fixture names the serving node and its successor
- [0026](docs/adr/0026-the-spring-boot-starter-binds-the-sdks-records-and-takes-functions-from-beans.md) The Spring Boot starter binds the SDK's records and takes functions from beans
- [0027](docs/adr/0027-spring-lives-only-in-the-spring-modules.md) Spring lives only in the Spring modules
- [0028](docs/adr/0028-connect-validates-options-against-the-drivers-schema.md) Connect validates options against the driver's schema
- [0029](docs/adr/0029-micrometer-owns-metrics-opentelemetry-owns-traces.md) Micrometer owns metrics, OpenTelemetry owns traces
- [0030](docs/adr/0030-connections-are-derived-from-spring-boots-connection-details.md) Connections are derived from Spring Boot's connection details

## Trademarks

Apache ActiveMQ, ActiveMQ Artemis and Artemis are trademarks of the Apache Software Foundation. Other
broker names are trademarks of their owners. OpenBroker is not affiliated with or endorsed by them; it
names them only to say which systems it works with. See [NOTICE](NOTICE).

Licensed under the [Apache License 2.0](LICENSE).
