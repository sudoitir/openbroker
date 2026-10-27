package io.github.sudoitir.openbroker.opentelemetry;

import static io.github.sudoitir.openbroker.opentelemetry.MessagingAttributes.*;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.LinkEvent;
import io.github.sudoitir.openbroker.messaging.LinkListener;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import io.github.sudoitir.openbroker.messaging.Observation;
import io.github.sudoitir.openbroker.messaging.OperationInfo;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.jspecify.annotations.Nullable;

/**
 * Traces and measures messaging with OpenTelemetry. Pass it to {@code ConnectOptions.withObserver};
 * it also hears link events as a {@link LinkListener}.
 *
 * <p>A send is a producer span whose context travels in the message's string properties through the
 * propagator of the given {@code OpenTelemetry}. A receive or process span links to the context it
 * finds in each message.
 */
public final class OpenTelemetryMessaging implements MessagingObserver, LinkListener {

    private static final String SCOPE = "io.github.sudoitir.openbroker";
    private static final List<Double> DURATION_BUCKETS =
            List.of(0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0);
    private static final TextMapSetter<BiConsumer<String, String>> SETTER = (carrier, key, value) -> {
        if (carrier != null) {
            carrier.accept(key, value);
        }
    };
    private static final TextMapGetter<Map<String, PropertyValue>> GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Map<String, PropertyValue> carrier) {
            return carrier.keySet();
        }

        @Override
        public @Nullable String get(@Nullable Map<String, PropertyValue> carrier, String key) {
            return carrier != null && carrier.get(key) instanceof PropertyValue.OfString s ? s.value() : null;
        }
    };

    private final Tracer tracer;
    private final TextMapPropagator propagator;
    private final DoubleHistogram operationDuration;
    private final DoubleHistogram processDuration;
    private final LongCounter sent;
    private final LongCounter consumed;
    private final LongCounter recoveries;
    private final boolean injectContext;

    private OpenTelemetryMessaging(OpenTelemetry openTelemetry, boolean injectContext) {
        this.injectContext = injectContext;
        this.tracer = openTelemetry.getTracer(SCOPE);
        this.propagator = openTelemetry.getPropagators().getTextMapPropagator();
        Meter meter = openTelemetry.getMeter(SCOPE);
        this.operationDuration = meter.histogramBuilder(OPERATION_DURATION)
                .setUnit("s")
                .setDescription("Duration of a messaging client operation.")
                .setExplicitBucketBoundariesAdvice(DURATION_BUCKETS)
                .build();
        this.processDuration = meter.histogramBuilder(PROCESS_DURATION)
                .setUnit("s")
                .setDescription("Duration of processing one message.")
                .setExplicitBucketBoundariesAdvice(DURATION_BUCKETS)
                .build();
        this.sent = meter.counterBuilder(SENT_MESSAGES)
                .setUnit("{message}")
                .setDescription("Messages a client sent.")
                .build();
        this.consumed = meter.counterBuilder(CONSUMED_MESSAGES)
                .setUnit("{message}")
                .setDescription("Messages a client consumed.")
                .build();
        this.recoveries = meter.counterBuilder(LINK_RECOVERIES)
                .setUnit("{event}")
                .setDescription("Link state changes.")
                .build();
    }

    /** Spans and metrics through {@code openTelemetry}, writing the trace context into each sent message. */
    public static OpenTelemetryMessaging of(OpenTelemetry openTelemetry) {
        return builder(openTelemetry).build();
    }

    public static Builder builder(OpenTelemetry openTelemetry) {
        return new Builder(openTelemetry);
    }

    /** Builds an observer with choices beyond {@link #of(OpenTelemetry)}. */
    public static final class Builder {

        private final OpenTelemetry openTelemetry;
        private boolean injectContext = true;

        private Builder(OpenTelemetry openTelemetry) {
            this.openTelemetry = Objects.requireNonNull(openTelemetry, "openTelemetry");
        }

        /**
         * Whether a send writes the trace context into the message (default {@code true}). Turn it off
         * in a tool that moves, retries or relays messages producers wrote: the send is still a span,
         * and the message keeps exactly the properties it had, including the producer's own context.
         */
        public Builder injectContext(boolean inject) {
            this.injectContext = inject;
            return this;
        }

        public OpenTelemetryMessaging build() {
            return new OpenTelemetryMessaging(openTelemetry, injectContext);
        }
    }

    @Override
    public Observation onSend(OperationInfo op, OutboundMessage message, BiConsumer<String, String> carrier) {
        Span span = start(op, SpanKind.PRODUCER, op.operation(), destinationName(op), null);
        message.headers().id().ifPresent(id -> span.setAttribute(MESSAGE_ID, id));
        message.headers().correlationId().ifPresent(id -> span.setAttribute(CONVERSATION_ID, id));
        span.setAttribute(BODY_SIZE, message.body().size());
        if (injectContext) {
            propagator.inject(Context.current().with(span), carrier, SETTER);
        }
        return new Operation(op, op.operation(), span, sent, operationDuration, op.messageCount());
    }

    @Override
    public Observation onReceive(OperationInfo op) {
        Span span = start(op, SpanKind.CLIENT, op.operation(), subscriptionName(op), null);
        return new Operation(op, op.operation(), span, consumed, operationDuration, 0) {
            @Override
            public void received(List<Message> messages) {
                span.setAttribute(BATCH_COUNT, (long) messages.size());
                for (Message m : messages) {
                    SpanContext creation = creationContext(m);
                    if (creation.isValid()) {
                        span.addLink(creation);
                    }
                }
                consumed.add(messages.size(), metricAttributes(op, op.operation(), null));
            }
        };
    }

    @Override
    public Observation onProcess(OperationInfo op, Message message) {
        SpanContext creation = creationContext(message);
        Span span = start(op, SpanKind.CONSUMER, op.operation(), subscriptionName(op), creation);
        message.headers().id().ifPresent(id -> span.setAttribute(MESSAGE_ID, id));
        return new Operation(op, op.operation(), span, consumed, processDuration, 1);
    }

    @Override
    public Observation onSettle(OperationInfo op, Message message, Disposition disposition) {
        String name =
                switch (disposition) {
                    case Disposition.Accept a -> "accept";
                    case Disposition.Release r -> "release";
                    case Disposition.Reject r -> "reject";
                };
        Span span = start(op, SpanKind.CLIENT, name, subscriptionName(op), null);
        message.headers().id().ifPresent(id -> span.setAttribute(MESSAGE_ID, id));
        return new Operation(op, name, span, null, operationDuration, 0);
    }

    @Override
    public void changed(LinkEvent event) {
        AttributesBuilder attributes =
                Attributes.builder().put(LINK_STATE, event.state().name().toLowerCase(Locale.ROOT));
        event.cause().ifPresent(cause -> attributes.put(ERROR_TYPE, errorType(cause)));
        recoveries.add(1, attributes.build());
    }

    private Span start(OperationInfo op, SpanKind kind, String operation, String name, @Nullable SpanContext creation) {
        SpanBuilder builder = tracer.spanBuilder(operation + " " + name).setSpanKind(kind);
        if (creation != null && creation.isValid()) {
            builder.addLink(creation);
        }
        return builder.setAllAttributes(metricAttributes(op, operation, null)).startSpan();
    }

    private SpanContext creationContext(Message message) {
        return Span.fromContext(propagator.extract(Context.root(), message.properties(), GETTER))
                .getSpanContext();
    }

    private static String destinationName(OperationInfo op) {
        return op.destination().path().name();
    }

    private static String subscriptionName(OperationInfo op) {
        return op.subscription().orElse(op.destination()).path().name();
    }

    private static Attributes metricAttributes(OperationInfo op, String operation, @Nullable Throwable error) {
        AttributesBuilder attributes = Attributes.builder()
                .put(SYSTEM, op.messagingSystem())
                .put(OPERATION_TYPE, op.type().name().toLowerCase(Locale.ROOT))
                .put(OPERATION_NAME, operation)
                .put(DESTINATION_NAME, destinationName(op));
        op.subscription()
                .ifPresent(s -> attributes.put(SUBSCRIPTION_NAME, s.path().name()));
        op.serverAddress().ifPresent(a -> attributes.put(SERVER_ADDRESS, a));
        if (op.serverPort().isPresent()) {
            attributes.put(SERVER_PORT, (long) op.serverPort().getAsInt());
        }
        if (error != null) {
            attributes.put(ERROR_TYPE, errorType(error));
        }
        return attributes.build();
    }

    private static String errorType(Throwable error) {
        return error instanceof BrokerException broker
                ? broker.reason()
                : error.getClass().getName();
    }

    /** One observed operation. */
    private static class Operation implements Observation {
        private final OperationInfo op;
        private final String operation;
        final Span span;
        private final @Nullable LongCounter counter;
        private final DoubleHistogram duration;
        private final long counted;
        private final long startNanos = System.nanoTime();

        Operation(
                OperationInfo op,
                String operation,
                Span span,
                @Nullable LongCounter counter,
                DoubleHistogram duration,
                long counted) {
            this.op = op;
            this.operation = operation;
            this.span = span;
            this.counter = counter;
            this.duration = duration;
            this.counted = counted;
        }

        @Override
        public AutoCloseable scope() {
            Scope scope = span.makeCurrent();
            return scope::close;
        }

        @Override
        public void end(@Nullable Throwable error) {
            Attributes attributes = metricAttributes(op, operation, error);
            if (error != null) {
                span.setAttribute(ERROR_TYPE, errorType(error));
                span.setStatus(StatusCode.ERROR);
                span.recordException(error);
            }
            span.end();
            duration.record((System.nanoTime() - startNanos) / (double) TimeUnit.SECONDS.toNanos(1), attributes);
            if (counter != null && counted > 0) {
                counter.add(counted, attributes);
            }
        }
    }
}
