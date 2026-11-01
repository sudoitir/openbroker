package io.github.sudoitir.openbroker.rabbitmq.admin;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.LongString;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.notification.BrokerEvent;
import io.github.sudoitir.openbroker.notification.Gap;
import io.github.sudoitir.openbroker.notification.NotificationListener;
import io.github.sudoitir.openbroker.notification.StreamState;
import io.github.sudoitir.openbroker.rabbitmq.RabbitMqOptions;
import io.github.sudoitir.openbroker.rabbitmq.amqp.AmqpConnections;
import io.github.sudoitir.openbroker.support.Deadlines;
import io.github.sudoitir.openbroker.support.NotificationDispatcher;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.jspecify.annotations.Nullable;

/**
 * Reads {@code amq.rabbitmq.event} through an exclusive server-named queue bound with {@code #} in the event virtual
 * host, maps the events, and reports a gap ({@code STARTED}, then {@code RECONNECTED} after every reopen).
 */
final class EventExchangeStream implements AutoCloseable {

    /** The exchange the event exchange plugin publishes to. */
    static final String EXCHANGE = "amq.rabbitmq.event";

    private static final int BUFFER = 1024;
    private static final Duration FIRST_RETRY = Duration.ofSeconds(1);

    private final RabbitMqCluster cluster;
    private final AmqpConnections connections;
    private final ScheduledExecutorService timer;
    private final NotificationDispatcher dispatcher;
    private final Map<String, KindId> queueKinds = new ConcurrentHashMap<>();
    private final AtomicLong sequence = new AtomicLong();
    private final AtomicBoolean reopening = new AtomicBoolean();
    private final String vhost;
    private volatile boolean closed;
    private volatile StreamState state;
    private volatile @Nullable Channel channel;
    private volatile String ownQueue = "";
    private @Nullable AutoCloseable loss;
    private int attempt;

    EventExchangeStream(RabbitMqCluster cluster, NotificationListener listener, AmqpConnections connections) {
        this(
                cluster,
                listener,
                connections,
                Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform()
                        .daemon()
                        .name("openbroker-rabbitmq-events")
                        .factory()));
    }

    /** A stream whose reopening {@code timer} schedules, so a test drives it. */
    EventExchangeStream(
            RabbitMqCluster cluster,
            NotificationListener listener,
            AmqpConnections connections,
            ScheduledExecutorService timer) {
        this.cluster = cluster;
        this.connections = connections;
        this.timer = timer;
        this.vhost = RabbitMqOptions.eventVhost(cluster.spec());
        this.dispatcher =
                new NotificationDispatcher(listener, BUFFER, cluster.options().clock(), r -> r);
        this.state = new StreamState(StreamState.Phase.CONNECTING, Optional.empty(), Optional.empty());
    }

    /**
     * Opens the queue and starts reading, and tells the listener the stream started.
     *
     * @throws BrokerException when the queue cannot be declared or bound, such as {@code PERMISSION_DENIED} when the
     *     account may not read the event exchange
     */
    void start() {
        try {
            open();
        } catch (BrokerException e) {
            state = new StreamState(StreamState.Phase.UNAVAILABLE, Optional.of(now()), Optional.of(e));
            close();
            throw e;
        }
        dispatcher.gap(new Gap(RabbitMqCluster.CLUSTER, now(), Gap.Cause.STARTED));
    }

    /** Where the stream is: connecting, active, or backing off until the connection is back. */
    StreamState state() {
        return state;
    }

    private void open() {
        try {
            Connection connection = connections.connection(vhost, Deadlines.current(cluster.options()));
            Channel opened = connection.createChannel();
            String queue = opened.queueDeclare("", false, true, true, null).getQueue();
            ownQueue = queue;
            opened.queueBind(queue, EXCHANGE, "#");
            opened.addShutdownListener(_ -> lost());
            opened.basicConsume(queue, true, this::deliver, _ -> lost());
            channel = opened;
            if (loss == null) {
                loss = connections.onLoss(vhost, this::lost);
            }
            state = new StreamState(StreamState.Phase.ACTIVE, Optional.of(now()), Optional.empty());
            attempt = 0;
        } catch (IOException | RuntimeException e) {
            if (e instanceof BrokerException broker) {
                throw broker;
            }
            throw RabbitMqErrors.fromIo(e, false);
        }
    }

    private void deliver(String consumerTag, Delivery delivery) {
        String routingKey = delivery.getEnvelope().getRoutingKey();
        Map<String, Object> headers = plain(delivery.getProperties().getHeaders());
        if (isOwn(routingKey, headers)) {
            return;
        }
        Optional<BrokerEvent> mapped =
                EventMapping.map(routingKey, headers, now(), sequence.incrementAndGet(), this::queueKind);
        mapped.ifPresent(event -> {
            event.subject().ifPresent(id -> remember(routingKey, id));
            dispatcher.event(event);
        });
    }

    /**
     * Whether the event is about the stream's own plumbing: its queue, its binding to the event exchange, or its
     * consumer. The stream must not report itself.
     */
    private boolean isOwn(String routingKey, Map<String, Object> headers) {
        String own = ownQueue;
        if (own.isEmpty()) {
            return false;
        }
        String queue;
        if (routingKey.startsWith("queue.")) {
            queue = String.valueOf(headers.get("name"));
        } else if (routingKey.startsWith("consumer.")) {
            queue = String.valueOf(headers.get("queue"));
        } else if (routingKey.startsWith("binding.")
                && !"exchange".equals(String.valueOf(headers.get("destination_kind")))) {
            queue = String.valueOf(headers.get("destination_name"));
        } else {
            return false;
        }
        return own.equals(queue) && vhost.equals(String.valueOf(headers.getOrDefault("vhost", "/")));
    }

    /** Keeps the kind of each queue created, so its deletion, which the listing no longer holds, names it right. */
    private void remember(String routingKey, ResourceId queue) {
        List<String> path = queue.path().segments();
        String key = path.getFirst() + "\u0000" + path.getLast();
        if ("queue.created".equals(routingKey)) {
            queueKinds.put(key, queue.kind());
        } else if ("queue.deleted".equals(routingKey)) {
            queueKinds.remove(key);
        }
    }

    private KindId queueKind(String queueVhost, String queue) {
        KindId known = queueKinds.get(queueVhost + "\u0000" + queue);
        if (known != null) {
            return known;
        }
        try {
            // Only a kind the broker confirmed is kept; a queue it no longer has is reported as classic and not cached.
            Optional<KindId> kind = cluster.queueTypes().knownKindOf(queueVhost, queue);
            kind.ifPresent(k -> queueKinds.put(queueVhost + "\u0000" + queue, k));
            return kind.orElse(RabbitMqKinds.CLASSIC_QUEUE);
        } catch (BrokerException _) {
            return RabbitMqKinds.CLASSIC_QUEUE; // A listing that fails must not lose the event.
        }
    }

    /** The connection or the channel went away: events in between are lost, so reopen and say so. */
    private void lost() {
        if (closed || !reopening.compareAndSet(false, true)) {
            return;
        }
        state = new StreamState(StreamState.Phase.BACKING_OFF, Optional.of(now()), Optional.empty());
        schedule();
    }

    private void schedule() {
        Duration cap = RabbitMqOptions.watchInterval(cluster.spec());
        Duration delay = FIRST_RETRY.multipliedBy(1L << Math.min(attempt, 6));
        attempt++;
        long millis = (delay.compareTo(cap) > 0 ? cap : delay).toMillis();
        timer.schedule(this::reopen, millis, TimeUnit.MILLISECONDS);
    }

    void reopen() {
        if (closed) {
            return;
        }
        try {
            open();
        } catch (BrokerException e) {
            state = new StreamState(StreamState.Phase.BACKING_OFF, Optional.of(now()), Optional.of(e));
            schedule();
            return;
        }
        reopening.set(false);
        dispatcher.gap(new Gap(RabbitMqCluster.CLUSTER, now(), Gap.Cause.RECONNECTED));
    }

    private Instant now() {
        return cluster.options().clock().instant();
    }

    /** The headers with every AMQP long string as text, so the mapping reads plain values. */
    private static Map<String, Object> plain(@Nullable Map<String, Object> headers) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (headers != null) {
            headers.forEach((name, value) -> out.put(name, plainValue(value)));
        }
        return out;
    }

    private static @Nullable Object plainValue(@Nullable Object value) {
        if (value instanceof LongString text) {
            return text.toString();
        }
        if (value instanceof Map<?, ?> table) {
            Map<String, Object> out = new LinkedHashMap<>();
            table.forEach((k, v) -> out.put(String.valueOf(k), plainValue(v)));
            return out;
        }
        return value;
    }

    @Override
    public void close() {
        closed = true;
        timer.shutdownNow();
        try {
            if (loss != null) {
                loss.close();
            }
        } catch (Exception _) {
            // The loss listener is removed with the connection anyway.
        }
        Channel open = channel;
        if (open != null) {
            try {
                open.close();
            } catch (IOException | RuntimeException | TimeoutException _) {
                // The channel is gone with its connection; a close that fails leaves nothing behind.
            }
        }
        dispatcher.close();
    }
}
