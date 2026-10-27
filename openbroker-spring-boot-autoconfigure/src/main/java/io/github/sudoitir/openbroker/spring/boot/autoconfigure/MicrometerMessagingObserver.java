package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.LinkEvent;
import io.github.sudoitir.openbroker.messaging.LinkListener;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import io.github.sudoitir.openbroker.messaging.Observation;
import io.github.sudoitir.openbroker.messaging.OperationInfo;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.jspecify.annotations.Nullable;

/**
 * Measures messaging with Micrometer. It records the instruments the OpenTelemetry observer records, under
 * the same names, tagged with the connection's name and system and the operation. The destination is not
 * a tag: reply, temporary and per-tenant destinations make it unbounded, and spans carry it instead.
 * Pass it to {@code ConnectOptions.withObserver}, and to {@code Transport.watch} to count link
 * recoveries.
 *
 * <ul>
 *   <li>{@code messaging.client.operation.duration}, a timer of every operation;
 *   <li>{@code messaging.process.duration}, a timer of handling one message;
 *   <li>{@code messaging.client.sent.messages} and {@code messaging.client.consumed.messages}, counters;
 *   <li>{@code openbroker.messaging.link.recoveries}, a counter of link state changes.
 * </ul>
 *
 * <p>Tracing is not its job: the observer never touches a message.
 */
public final class MicrometerMessagingObserver implements MessagingObserver, LinkListener {

    static final String OPERATION_DURATION = "messaging.client.operation.duration";
    static final String PROCESS_DURATION = "messaging.process.duration";
    static final String SENT_MESSAGES = "messaging.client.sent.messages";
    static final String CONSUMED_MESSAGES = "messaging.client.consumed.messages";
    static final String LINK_RECOVERIES = "openbroker.messaging.link.recoveries";

    private static final String NONE = "none";

    private final MeterRegistry registry;
    private final Tags connection;

    /**
     * @param registry where the instruments are recorded
     * @param connection the connection's name, the tag {@code openbroker.connection}
     * @param system the broker system, the tag {@code openbroker.system}
     */
    public MicrometerMessagingObserver(MeterRegistry registry, String connection, String system) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.connection = Tags.of("openbroker.connection", connection, "openbroker.system", system);
    }

    @Override
    public Observation onSend(OperationInfo op, OutboundMessage message, BiConsumer<String, String> carrier) {
        return new Operation(op, op.operation(), SENT_MESSAGES, OPERATION_DURATION, op.messageCount());
    }

    @Override
    public Observation onReceive(OperationInfo op) {
        return new Operation(op, op.operation(), CONSUMED_MESSAGES, OPERATION_DURATION, 0) {
            @Override
            public void received(List<Message> messages) {
                registry.counter(CONSUMED_MESSAGES, tags(op, op.operation(), null))
                        .increment(messages.size());
            }
        };
    }

    @Override
    public Observation onProcess(OperationInfo op, Message message) {
        return new Operation(op, op.operation(), CONSUMED_MESSAGES, PROCESS_DURATION, 1);
    }

    @Override
    public Observation onSettle(OperationInfo op, Message message, Disposition disposition) {
        String name =
                switch (disposition) {
                    case Disposition.Accept _ -> "accept";
                    case Disposition.Release _ -> "release";
                    case Disposition.Reject _ -> "reject";
                };
        return new Operation(op, name, null, OPERATION_DURATION, 0);
    }

    @Override
    public void changed(LinkEvent event) {
        registry.counter(
                        LINK_RECOVERIES,
                        connection.and(
                                "openbroker.link.state",
                                event.state().name().toLowerCase(Locale.ROOT),
                                "error.type",
                                event.cause()
                                        .map(MicrometerMessagingObserver::errorType)
                                        .orElse(NONE)))
                .increment();
    }

    private Tags tags(OperationInfo op, String operation, @Nullable Throwable error) {
        return connection.and(
                "messaging.system",
                op.messagingSystem(),
                "messaging.operation.type",
                op.type().name().toLowerCase(Locale.ROOT),
                "messaging.operation.name",
                operation,
                "error.type",
                error == null ? NONE : errorType(error));
    }

    private static String errorType(Throwable error) {
        return error instanceof BrokerException broker
                ? broker.reason()
                : error.getClass().getName();
    }

    /** One observed operation. */
    private class Operation implements Observation {
        private final OperationInfo op;
        private final String operationName;
        private final @Nullable String counter;
        private final String duration;
        private final long counted;
        private final long startNanos = System.nanoTime();

        Operation(OperationInfo op, String operation, @Nullable String counter, String duration, long counted) {
            this.op = op;
            this.operationName = operation;
            this.counter = counter;
            this.duration = duration;
            this.counted = counted;
        }

        @Override
        public void end(@Nullable Throwable error) {
            Tags tags = tags(op, operationName, error);
            Timer.builder(duration)
                    .description("Duration of a messaging client operation.")
                    .tags(tags)
                    .register(registry)
                    .record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
            if (counter != null && counted > 0) {
                registry.counter(counter, tags).increment(counted);
            }
        }
    }
}
