package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.TlsProvider;
import io.github.sudoitir.openbroker.messaging.LinkEvent;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.MessageRef;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import io.github.sudoitir.openbroker.messaging.Observation;
import io.github.sudoitir.openbroker.messaging.OperationInfo;
import io.github.sudoitir.openbroker.messaging.OperationType;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Shard;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.ssl.SslAutoConfiguration;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class ObservabilityTest {

    private static final ResourceId QUEUE = ResourceId.of("artemis.queue", "orders");

    private final FakeDriver driver = new FakeDriver();
    private final InMemorySpanExporter spans = InMemorySpanExporter.create();
    private final InMemoryMetricReader metrics = InMemoryMetricReader.create();
    private final OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
            .setTracerProvider(SdkTracerProvider.builder()
                    .addSpanProcessor(SimpleSpanProcessor.create(spans))
                    .build())
            .setMeterProvider(
                    SdkMeterProvider.builder().registerMetricReader(metrics).build())
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .build();
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(SslAutoConfiguration.class, OpenBrokerAutoConfiguration.class))
            .withBean(BrokerDrivers.class, () -> BrokerDrivers.of(List.of(driver)));

    private static OperationInfo info(OperationType type, String operation) {
        return new OperationInfo(
                "artemis",
                type,
                operation,
                QUEUE,
                Optional.empty(),
                NodeId.of("n1"),
                Optional.of("broker.example"),
                OptionalInt.of(61616),
                1);
    }

    private static Message received(Map<String, PropertyValue> properties) {
        return new Message(
                new MessageRef(QUEUE, Shard.node(NodeId.of("n1")), "1"),
                MessageHeaders.builder().id("m-1").build(),
                properties,
                MessageBody.of(new byte[] {1}),
                Set.of(),
                Map.of());
    }

    /** Sends one message through the observer and returns what the observer wrote into it. */
    private static Map<String, PropertyValue> send(MessagingObserver observer) {
        Map<String, String> carrier = new HashMap<>();
        Observation send = observer.onSend(
                info(OperationType.SEND, "send"),
                new OutboundMessage(
                        MessageHeaders.builder().id("m-1").build(),
                        Map.of(),
                        MessageBody.of(new byte[] {1, 2}),
                        Map.of()),
                carrier::put);
        send.end(null);
        Map<String, PropertyValue> properties = new HashMap<>();
        carrier.forEach((key, value) -> properties.put(key, PropertyValue.of(value)));
        return properties;
    }

    private static void process(MessagingObserver observer, Map<String, PropertyValue> properties) {
        observer.onProcess(info(OperationType.PROCESS, "process"), received(properties))
                .end(null);
    }

    private MessagingObserver observerOf(AssertableApplicationContext context) {
        return context.getBean(OpenBrokerConnectOptions.class)
                .forConnection("c1", "artemis", TlsProvider.none())
                .observer();
    }

    @Test
    void withNeitherMicrometerNorOpenTelemetryNothingIsWired() {
        runner.withClassLoader(new FilteredClassLoader(MeterRegistry.class, OpenTelemetry.class))
                .run(context -> {
                    assertThat(context).doesNotHaveBean(ObserverSource.class);
                    assertThat(observerOf(context)).isSameAs(MessagingObserver.none());
                });
        runner.run(context -> {
            assertThat(context).doesNotHaveBean(ObserverSource.class);
            assertThat(observerOf(context)).isSameAs(MessagingObserver.none());
        });
    }

    @Test
    void micrometerRecordsTheMetricsTaggedWithTheConnection() {
        MeterRegistry registry = new SimpleMeterRegistry();
        runner.withBean(MeterRegistry.class, () -> registry).run(context -> {
            MessagingObserver observer = observerOf(context);

            process(observer, send(observer));

            assertThat(registry.get("messaging.client.operation.duration")
                            .tag("openbroker.connection", "c1")
                            .tag("openbroker.system", "artemis")
                            .tag("messaging.operation.name", "send")
                            .tag("messaging.destination.name", "orders")
                            .timer()
                            .count())
                    .isEqualTo(1);
            assertThat(registry.get("messaging.client.sent.messages").counter().count())
                    .isEqualTo(1);
            assertThat(registry.get("messaging.process.duration").timer().count())
                    .isEqualTo(1);
            assertThat(registry.get("messaging.client.consumed.messages")
                            .counter()
                            .count())
                    .isEqualTo(1);
        });
    }

    @Test
    void anErrorIsATagAndAFailedSendIsNotACountOfSuccess() {
        MeterRegistry registry = new SimpleMeterRegistry();
        runner.withBean(MeterRegistry.class, () -> registry).run(context -> {
            observerOf(context)
                    .onSend(
                            info(OperationType.SEND, "send"),
                            new OutboundMessage(
                                    MessageHeaders.builder().id("m").build(),
                                    Map.of(),
                                    MessageBody.of(new byte[0]),
                                    Map.of()),
                            (k, v) -> {})
                    .end(new IllegalStateException("boom"));

            assertThat(registry.get("messaging.client.operation.duration")
                            .tag("error.type", IllegalStateException.class.getName())
                            .timer()
                            .count())
                    .isEqualTo(1);
        });
    }

    @Test
    void aConfiguredConnectionsTransportIsWatchedForLinkRecoveries() {
        MeterRegistry registry = new SimpleMeterRegistry();
        runner.withBean(MeterRegistry.class, () -> registry)
                .withPropertyValues("openbroker.connections.orders.system=fake")
                .run(context -> {
                    assertThat(driver.watchers).hasSize(1);

                    driver.watchers
                            .getFirst()
                            .changed(new LinkEvent(
                                    "l1",
                                    NodeId.of("n1"),
                                    LinkState.RECOVERING,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Instant.EPOCH));

                    assertThat(registry.get("openbroker.messaging.link.recoveries")
                                    .tag("openbroker.connection", "orders")
                                    .tag("openbroker.link.state", "recovering")
                                    .counter()
                                    .count())
                            .isEqualTo(1);
                });
    }

    @Test
    void openTelemetryAloneEmitsSpansAndMetrics() {
        runner.withBean(OpenTelemetry.class, () -> sdk).run(context -> {
            MessagingObserver observer = observerOf(context);

            Map<String, PropertyValue> properties = send(observer);
            process(observer, properties);

            assertThat(spans.getFinishedSpanItems())
                    .extracting(SpanData::getName)
                    .contains("send orders", "process orders");
            SpanData send = span("send orders");
            SpanData process = span("process orders");
            assertThat(send.getKind()).isEqualTo(SpanKind.PRODUCER);
            assertThat(process.getKind()).isEqualTo(SpanKind.CONSUMER);
            assertThat(process.getLinks())
                    .anySatisfy(link -> assertThat(link.getSpanContext().getTraceId())
                            .isEqualTo(send.getSpanContext().getTraceId()));
            assertThat(metrics.collectAllMetrics())
                    .extracting(m -> m.getName())
                    .contains("messaging.client.operation.duration");
        });
    }

    @Test
    void withBothTheMetricsAreRecordedOnceInMicrometerAndTheSpansInOpenTelemetry() {
        MeterRegistry registry = new SimpleMeterRegistry();
        runner.withBean(MeterRegistry.class, () -> registry)
                .withBean(OpenTelemetry.class, () -> sdk)
                .run(context -> {
                    MessagingObserver observer = observerOf(context);

                    send(observer);

                    assertThat(registry.get("messaging.client.sent.messages")
                                    .counter()
                                    .count())
                            .isEqualTo(1);
                    assertThat(metrics.collectAllMetrics()).isEmpty();
                    assertThat(spans.getFinishedSpanItems())
                            .extracting(SpanData::getName)
                            .containsExactly("send orders");
                });
    }

    @Test
    void theTraceContextIsInjectedByDefault() {
        runner.withBean(OpenTelemetry.class, () -> sdk)
                .run(context -> assertThat(send(observerOf(context))).containsKey("traceparent"));
    }

    @Test
    void injectionOfTheTraceContextCanBeTurnedOff() {
        runner.withBean(OpenTelemetry.class, () -> sdk)
                .withPropertyValues("openbroker.tracing.inject-context=false")
                .run(context -> {
                    assertThat(send(observerOf(context))).doesNotContainKey("traceparent");
                    assertThat(spans.getFinishedSpanItems())
                            .extracting(SpanData::getName)
                            .contains("send orders");
                });
    }

    private SpanData span(String name) {
        return spans.getFinishedSpanItems().stream()
                .filter(s -> s.getName().equals(name))
                .findFirst()
                .orElseThrow();
    }
}
