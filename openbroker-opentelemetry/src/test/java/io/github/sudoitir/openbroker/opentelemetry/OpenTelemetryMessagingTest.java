package io.github.sudoitir.openbroker.opentelemetry;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.LinkEvent;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.MessageRef;
import io.github.sudoitir.openbroker.messaging.Observation;
import io.github.sudoitir.openbroker.messaging.OperationInfo;
import io.github.sudoitir.openbroker.messaging.OperationType;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Shard;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OpenTelemetryMessagingTest {

    private static final ResourceId QUEUE = ResourceId.of("artemis.queue", "orders");
    private static final ResourceId SUBSCRIPTION = ResourceId.of("artemis.queue", "orders-sub");

    private InMemorySpanExporter spans;
    private InMemoryMetricReader metrics;
    private OpenTelemetrySdk sdk;
    private OpenTelemetryMessaging observer;

    @BeforeEach
    void setUp() {
        spans = InMemorySpanExporter.create();
        metrics = InMemoryMetricReader.create();
        sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(SdkTracerProvider.builder()
                        .addSpanProcessor(SimpleSpanProcessor.create(spans))
                        .build())
                .setMeterProvider(
                        SdkMeterProvider.builder().registerMetricReader(metrics).build())
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
        observer = OpenTelemetryMessaging.of(sdk);
    }

    private static OperationInfo info(OperationType type, String operation, boolean subscribed) {
        return new OperationInfo(
                "activemq",
                type,
                operation,
                QUEUE,
                subscribed ? Optional.of(SUBSCRIPTION) : Optional.empty(),
                NodeId.of("n1"),
                Optional.of("broker.example"),
                OptionalInt.of(5672),
                1);
    }

    private static Message received(Map<String, PropertyValue> properties) {
        return new Message(
                new MessageRef(SUBSCRIPTION, Shard.node(NodeId.of("n1")), "1"),
                MessageHeaders.builder().id("m-1").build(),
                properties,
                MessageBody.of(new byte[] {1}),
                Set.of(),
                Map.of());
    }

    private Map<String, PropertyValue> send(String id) {
        Map<String, String> carrier = new HashMap<>();
        Observation send = observer.onSend(
                info(OperationType.SEND, "send", false),
                new OutboundMessage(
                        MessageHeaders.builder().id(id).build(), Map.of(), MessageBody.of(new byte[] {1, 2}), Map.of()),
                carrier::put);
        send.end(null);
        Map<String, PropertyValue> properties = new HashMap<>();
        carrier.forEach((k, v) -> properties.put(k, PropertyValue.of(v)));
        return properties;
    }

    private SpanData span(String name) {
        return spans.getFinishedSpanItems().stream()
                .filter(s -> s.getName().equals(name))
                .findFirst()
                .orElseThrow();
    }

    private MetricData metric(String name) {
        return metrics.collectAllMetrics().stream()
                .filter(m -> m.getName().equals(name))
                .findFirst()
                .orElseThrow();
    }

    @Test
    void sendIsAProducerSpanWithAttributesAndInjectsTheContext() {
        Map<String, PropertyValue> properties = send("m-1");

        SpanData span = span("send orders");
        assertThat(span.getKind()).isEqualTo(SpanKind.PRODUCER);
        assertThat(span.getAttributes().asMap())
                .extractingByKey(MessagingAttributes.SYSTEM)
                .isEqualTo("activemq");
        assertThat(span.getAttributes().get(MessagingAttributes.OPERATION_TYPE)).isEqualTo("send");
        assertThat(span.getAttributes().get(MessagingAttributes.DESTINATION_NAME))
                .isEqualTo("orders");
        assertThat(span.getAttributes().get(MessagingAttributes.MESSAGE_ID)).isEqualTo("m-1");
        assertThat(span.getAttributes().get(MessagingAttributes.BODY_SIZE)).isEqualTo(2L);
        assertThat(span.getAttributes().get(MessagingAttributes.SERVER_PORT)).isEqualTo(5672L);
        assertThat(properties).containsKey("traceparent");
    }

    @Test
    void withoutInjectionASendIsStillASpanAndWritesNothingIntoTheMessage() {
        observer = OpenTelemetryMessaging.builder(sdk).injectContext(false).build();

        Map<String, PropertyValue> properties = send("m-2");

        assertThat(span("send orders").getKind()).isEqualTo(SpanKind.PRODUCER);
        assertThat(properties).isEmpty();
    }

    @Test
    void processLinksToTheSendThroughAPropertyRoundTrip() throws Exception {
        Map<String, PropertyValue> properties = send("m-1");
        SpanData sent = span("send orders");

        Observation process = observer.onProcess(info(OperationType.PROCESS, "process", true), received(properties));
        try (AutoCloseable scope = process.scope()) {
            assertThat(io.opentelemetry.api.trace.Span.current()
                            .getSpanContext()
                            .isValid())
                    .isTrue();
        }
        process.end(null);

        SpanData processed = span("process orders-sub");
        assertThat(processed.getKind()).isEqualTo(SpanKind.CONSUMER);
        assertThat(processed.getLinks()).hasSize(1);
        assertThat(processed.getLinks().getFirst().getSpanContext().getSpanId()).isEqualTo(sent.getSpanId());
        assertThat(processed.getAttributes().get(MessagingAttributes.MESSAGE_ID))
                .isEqualTo("m-1");
    }

    @Test
    void receiveLinksEachMessageAndCountsThem() {
        Map<String, PropertyValue> properties = send("m-1");
        SpanData sent = span("send orders");

        Observation receive = observer.onReceive(info(OperationType.RECEIVE, "receive", true));
        receive.received(List.of(received(properties), received(Map.of())));
        receive.end(null);

        SpanData span = span("receive orders-sub");
        assertThat(span.getKind()).isEqualTo(SpanKind.CLIENT);
        assertThat(span.getAttributes().get(MessagingAttributes.BATCH_COUNT)).isEqualTo(2L);
        assertThat(span.getAttributes().get(MessagingAttributes.SUBSCRIPTION_NAME))
                .isEqualTo("orders-sub");
        assertThat(span.getLinks()).hasSize(1);
        assertThat(span.getLinks().getFirst().getSpanContext().getSpanId()).isEqualTo(sent.getSpanId());
        assertThat(metric("messaging.client.consumed.messages").getLongSumData().getPoints())
                .anyMatch(p -> p.getValue() == 2);
    }

    @Test
    void settleNamesTheDisposition() {
        observer.onSettle(
                        info(OperationType.SETTLE, "settle", true),
                        received(Map.of()),
                        new Disposition.Reject(Optional.empty()))
                .end(null);

        SpanData span = span("reject orders-sub");
        assertThat(span.getKind()).isEqualTo(SpanKind.CLIENT);
        assertThat(span.getAttributes().get(MessagingAttributes.OPERATION_TYPE)).isEqualTo("settle");
        assertThat(span.getAttributes().get(MessagingAttributes.OPERATION_NAME)).isEqualTo("reject");
    }

    @Test
    void metricsAreRecorded() {
        send("m-1");
        Observation process = observer.onProcess(info(OperationType.PROCESS, "process", true), received(Map.of()));
        process.end(null);

        assertThat(metric("messaging.client.operation.duration")
                        .getHistogramData()
                        .getPoints())
                .hasSize(1);
        assertThat(metric("messaging.client.sent.messages").getLongSumData().getPoints())
                .singleElement()
                .satisfies(p -> assertThat(p.getValue()).isEqualTo(1));
        assertThat(metric("messaging.process.duration").getHistogramData().getPoints())
                .singleElement()
                .satisfies(p ->
                        assertThat(p.getBoundaries()).startsWith(0.005, 0.01).endsWith(7.5, 10.0));
        assertThat(metric("messaging.client.consumed.messages").getLongSumData().getPoints())
                .singleElement()
                .satisfies(p -> assertThat(p.getValue()).isEqualTo(1));
    }

    @Test
    void errorTypeIsTheBrokerReasonOrTheExceptionClass() {
        BrokerException broker = new BrokerException(
                Status.UNAVAILABLE, ErrorInfo.of("SEND_BUFFER_FULL"), Dispatch.NOT_SENT, null, "full");
        observer.onSend(info(OperationType.SEND, "send", false), OutboundMessage.of(MessageBody.empty()), (k, v) -> {})
                .end(broker);
        observer.onProcess(info(OperationType.PROCESS, "process", true), received(Map.of()))
                .end(new IllegalStateException("boom"));

        assertThat(span("send orders").getAttributes().get(MessagingAttributes.ERROR_TYPE))
                .isEqualTo("SEND_BUFFER_FULL");
        assertThat(span("send orders").getStatus().getStatusCode())
                .isEqualTo(io.opentelemetry.api.trace.StatusCode.ERROR);
        assertThat(span("process orders-sub").getAttributes().get(MessagingAttributes.ERROR_TYPE))
                .isEqualTo("java.lang.IllegalStateException");
        assertThat(metric("messaging.client.sent.messages").getLongSumData().getPoints())
                .singleElement()
                .satisfies(p -> assertThat(p.getAttributes().get(MessagingAttributes.ERROR_TYPE))
                        .isEqualTo("SEND_BUFFER_FULL"));
    }

    @Test
    void linkEventsCountRecoveries() {
        observer.changed(new LinkEvent(
                "l1",
                NodeId.of("n1"),
                LinkState.RECOVERING,
                Optional.empty(),
                Optional.empty(),
                java.time.Instant.EPOCH));

        assertThat(metric("openbroker.messaging.link.recoveries")
                        .getLongSumData()
                        .getPoints())
                .singleElement()
                .satisfies(p -> assertThat(p.getAttributes().get(MessagingAttributes.LINK_STATE))
                        .isEqualTo("recovering"));
    }
}
