# OpenBroker

One Java SDK, and one vocabulary, for every message broker and queue service.

OpenBroker lets a backend inspect, operate and move messages on Apache ActiveMQ Artemis today,
and on Kafka, RabbitMQ, ActiveMQ Classic and others as their drivers arrive, through one small,
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
| `openbroker-bom` | Aligns the versions above. |

Java 25. The modules ship `module-info` descriptors (the driver an `Automatic-Module-Name`).

## Quick start

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>io.github.sudoitir</groupId>
      <artifactId>openbroker-bom</artifactId>
      <version>0.1.0</version>
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
                CredentialSource.of(Credentials.of("admin", System.getenv("BROKER_PASSWORD"))),
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
- **What only one system has** is reached through `cluster.unwrap(…)`, for Artemis
  `ArtemisManagement`.

## Writing a driver

Implement `io.github.sudoitir.openbroker.spi.BrokerDriver`, register it in
`META-INF/services/io.github.sudoitir.openbroker.spi.BrokerDriver`, and extend the TCK's contract
tests for the capabilities you declare. `openbroker-artemis` is the reference.

## Decisions

The design is recorded in [`docs/adr/`](docs/adr/).

## Trademarks

Apache ActiveMQ, ActiveMQ Artemis and Artemis are trademarks of the Apache Software Foundation. Other
broker names are trademarks of their owners. OpenBroker is not affiliated with or endorsed by them; it
names them only to say which systems it works with. See [NOTICE](NOTICE).

Licensed under the [Apache License 2.0](LICENSE).
