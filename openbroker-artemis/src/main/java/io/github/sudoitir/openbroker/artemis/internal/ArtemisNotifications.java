package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.artemis.ArtemisKinds;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import io.github.sudoitir.openbroker.notification.BrokerEvent;
import io.github.sudoitir.openbroker.notification.EventTypes;
import io.github.sudoitir.openbroker.notification.Gap;
import io.github.sudoitir.openbroker.notification.NotificationListener;
import io.github.sudoitir.openbroker.notification.Notifications;
import io.github.sudoitir.openbroker.notification.StreamState;
import io.github.sudoitir.openbroker.support.NotificationDispatcher;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.SSLException;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.jspecify.annotations.Nullable;

/**
 * The broker's {@code activemq.notifications} over Core, one subscription per node. Each node's
 * stream runs on its own virtual thread, polling {@code receive} (a message listener deadlocks
 * against {@code close} on some client and broker pairings), and reconnects with backoff from one
 * second to five minutes. A reconnect is a {@link Gap}: the broker replays nothing.
 */
public final class ArtemisNotifications implements Notifications {

    private static final String ADDRESS = "activemq.notifications";
    private static final int BUFFER = 10_000;
    private static final Duration RECEIVE = Duration.ofMillis(250);
    private static final Duration RETRY_FIRST = Duration.ofSeconds(1);
    private static final Duration RETRY_MAX = Duration.ofMinutes(5);

    /** Data keys: the broker's notification type, and every {@code _AMQ_} property under its own name. */
    public static final String TYPE = "artemis.notification_type";

    public static final String PROPERTY_PREFIX = "artemis.";

    private final Cluster cluster;
    private final Map<NodeId, StreamState> states = new ConcurrentHashMap<>();
    private final Set<AutoCloseable> subscriptions = ConcurrentHashMap.newKeySet();

    ArtemisNotifications(Cluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public AutoCloseable subscribe(Scope scope, NotificationListener listener) {
        NotificationDispatcher dispatcher = new NotificationDispatcher(
                listener, BUFFER, cluster.options().clock(), cluster.options().contextPropagation());
        List<Stream> streams = new ArrayList<>();
        for (NodeHandle node : cluster.nodes(scope)) {
            Stream stream = new Stream(node, dispatcher);
            streams.add(stream);
            stream.start();
        }
        AutoCloseable subscription = new AutoCloseable() {
            @Override
            public void close() {
                subscriptions.remove(this);
                streams.forEach(Stream::close);
                dispatcher.close();
            }
        };
        subscriptions.add(subscription);
        return subscription;
    }

    /** Closes every subscription still open, when the connection closes. */
    void closeAll() {
        for (AutoCloseable subscription : Set.copyOf(subscriptions)) {
            try {
                subscription.close();
            } catch (Exception e) {
                // teardown
            }
        }
    }

    @Override
    public Map<NodeId, StreamState> state() {
        return Map.copyOf(states);
    }

    /** One node's subscription and its reconnect loop. */
    private final class Stream {

        private final NodeHandle node;
        private final NotificationDispatcher dispatcher;
        private volatile boolean running = true;
        private volatile @Nullable Connection connection;
        private @Nullable Thread thread;

        Stream(NodeHandle node, NotificationDispatcher dispatcher) {
            this.node = node;
            this.dispatcher = dispatcher;
        }

        void start() {
            thread = Thread.ofVirtual()
                    .name("openbroker-notifications-" + node.id())
                    .start(this::run);
        }

        private void run() {
            Duration backoff = RETRY_FIRST;
            Gap.Cause cause = Gap.Cause.STARTED;
            while (running) {
                Instant since = cluster.options().clock().instant();
                state(new StreamState(StreamState.Phase.CONNECTING, Optional.of(since), Optional.empty()));
                try {
                    if (node.core().isEmpty()) {
                        throw unavailable(ErrorReasons.ENDPOINT_NOT_FOUND, "This node has no Core endpoint.");
                    }
                    consume(since, cause);
                    backoff = RETRY_FIRST;
                } catch (BrokerException e) {
                    if (!running) {
                        return;
                    }
                    boolean refused = ErrorReasons.NOT_AUTHORIZED.equals(e.reason())
                            || ErrorReasons.CREDENTIALS_REJECTED.equals(e.reason())
                            || ErrorReasons.ENDPOINT_NOT_FOUND.equals(e.reason());
                    state(new StreamState(
                            refused ? StreamState.Phase.UNAVAILABLE : StreamState.Phase.BACKING_OFF,
                            Optional.of(cluster.options().clock().instant()),
                            Optional.of(e)));
                    sleep(backoff);
                    backoff = backoff.multipliedBy(2).compareTo(RETRY_MAX) > 0 ? RETRY_MAX : backoff.multipliedBy(2);
                }
                cause = Gap.Cause.RECONNECTED;
            }
        }

        /** Subscribes and delivers until the stream fails or closes. */
        private void consume(Instant since, Gap.Cause cause) {
            ActiveMQConnectionFactory factory = CoreLinks.connectionFactory(
                    cluster.options(),
                    node.core().orElseThrow(),
                    cluster.options().tls().current());
            Credentials account = cluster.credentials().forPlane(Plane.DATA);
            try (factory) {
                Connection opened = account.isNone()
                        ? factory.createConnection()
                        : factory.createConnection(
                                account.username(), account.password().reveal());
                connection = opened;
                try (opened) {
                    opened.start();
                    Session session = opened.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    MessageConsumer consumer = session.createConsumer(session.createTopic(ADDRESS));
                    state(new StreamState(StreamState.Phase.ACTIVE, Optional.of(since), Optional.empty()));
                    dispatcher.gap(new Gap(node.id(), since, cause));
                    while (running) {
                        Message message = consumer.receive(RECEIVE.toMillis());
                        if (message != null && !aboutItself(message)) {
                            dispatcher.event(event(node.id(), message));
                        }
                    }
                }
            } catch (JMSException e) {
                throw classify(e);
            } finally {
                connection = null;
            }
        }

        private void state(StreamState state) {
            states.put(node.id(), state);
        }

        private void sleep(Duration duration) {
            try {
                Thread.sleep(duration);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                running = false;
            }
        }

        private BrokerException unavailable(String reason, String message) {
            return new BrokerException(
                    Status.FAILED_PRECONDITION, ErrorInfo.of(reason), Dispatch.NOT_SENT, node.id(), message);
        }

        private BrokerException classify(JMSException e) {
            String message = String.valueOf(e.getMessage());
            if (message.contains("AMQ229213")) {
                // The account lacks consume or createNonDurableQueue on activemq.notifications.
                return new BrokerException(
                        Status.PERMISSION_DENIED,
                        ErrorInfo.of(ErrorReasons.NOT_AUTHORIZED),
                        Dispatch.REJECTED,
                        node.id(),
                        "The broker refused the subscription: " + message);
            }
            if (message.contains("AMQ229031")
                    || message.toLowerCase(Locale.ROOT).contains("security")) {
                return Errors.credentialsRejected(node.id(), Plane.DATA, 401, Errors.CREDENTIALS_MESSAGE);
            }
            for (Throwable c = e; c != null; c = c.getCause() == c ? null : c.getCause()) {
                if (c instanceof SSLException) {
                    return Errors.tls(node.id(), Errors.TLS_MESSAGE, e);
                }
                if (c instanceof ConnectException || c instanceof UnknownHostException) {
                    return Errors.unreachable(
                            node.id(), "Could not reach the broker over Core: " + message, Dispatch.NOT_SENT, e);
                }
            }
            return Errors.unreachable(node.id(), "The notification stream failed: " + message, Dispatch.IN_DOUBT, e);
        }

        void close() {
            running = false;
            Connection open = connection;
            if (open != null) {
                try {
                    open.close();
                } catch (JMSException e) {
                    // teardown
                }
            }
            if (thread != null) {
                thread.interrupt();
                try {
                    thread.join(Duration.ofSeconds(2));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            states.remove(node.id());
        }
    }

    /**
     * Whether a notification is about the notification address itself: a subscriber's own queue coming
     * and going, which is this stream's plumbing and nothing that happened on the broker.
     */
    static boolean aboutItself(Message message) throws JMSException {
        return ADDRESS.equals(message.getStringProperty("_AMQ_Address"));
    }

    /**
     * A notification as an event. Everything is read from its {@code _AMQ_} properties; a
     * notification has no message id. What it involves goes under the standard keys, every property
     * also under its own name, and a type this driver does not know still becomes an event.
     */
    static BrokerEvent event(NodeId node, Message message) throws JMSException {
        Map<String, Object> props = new LinkedHashMap<>();
        Enumeration<?> names = message.getPropertyNames();
        while (names.hasMoreElements()) {
            String name = (String) names.nextElement();
            Object value = message.getObjectProperty(name);
            if (value != null) {
                props.put(name, value instanceof Number || value instanceof Boolean ? value : value.toString());
            }
        }
        Attributes.Builder data = Attributes.builder();
        props.forEach((name, value) -> data.putRaw(PROPERTY_PREFIX + name, value));
        String type = text(props.get("_AMQ_NotifType")).orElse("");
        data.putRaw(TYPE, type);
        Optional<ResourceId> address = id(ArtemisKinds.ADDRESS, props.get("_AMQ_Address"));
        Optional<ResourceId> queue = id(ArtemisKinds.QUEUE, props.get("_AMQ_RoutingName"));
        Optional<ResourceId> connection = id(ArtemisKinds.CONNECTION, props.get("_AMQ_ConnectionName"));
        Optional<ResourceId> session = id(ArtemisKinds.SESSION, props.get("_AMQ_SessionName"));
        Optional<ResourceId> consumer = session.flatMap(s -> text(props.get("_AMQ_ConsumerName"))
                .map(c -> new ResourceId(ArtemisKinds.CONSUMER, ResourcePath.of(s.name(), c))));
        address.ifPresent(v -> data.put(StandardAttributes.DESTINATION, v));
        queue.ifPresent(v -> data.put(StandardAttributes.SUBSCRIPTION, v));
        connection.ifPresent(v -> data.put(StandardAttributes.CONNECTION, v));
        session.ifPresent(v -> data.put(StandardAttributes.CHANNEL, v));
        consumer.ifPresent(v -> data.put(StandardAttributes.CONSUMER, v));
        text(props.get("_AMQ_RemoteAddress")).ifPresent(v -> data.put(StandardAttributes.REMOTE_ADDRESS, v));
        text(props.getOrDefault("_AMQ_ValidatedUser", props.get("_AMQ_User")))
                .ifPresent(v -> data.put(StandardAttributes.PRINCIPAL, v));
        text(props.get("_AMQ_Message_ID")).ifPresent(v -> data.put(StandardAttributes.MESSAGE_ID, v));
        if (props.get("_AMQ_ConsumerCount") instanceof Number count) {
            data.put(StandardAttributes.CONSUMER_COUNT, count.longValue());
        }
        Optional<ResourceId> subject =
                switch (type) {
                    case "BINDING_ADDED", "BINDING_REMOVED", "MESSAGE_DELIVERED", "MESSAGE_EXPIRED" ->
                        queue.or(() -> address);
                    case "CONSUMER_CREATED", "CONSUMER_CLOSED", "CONSUMER_SLOW" -> consumer.or(() -> queue);
                    case "CONNECTION_CREATED", "CONNECTION_DESTROYED" -> connection;
                    case "SESSION_CREATED", "SESSION_CLOSED" -> session;
                    default -> address;
                };
        long millis = props.get("_AMQ_NotifTimestamp") instanceof Number n ? n.longValue() : message.getJMSTimestamp();
        return new BrokerEvent(
                UUID.randomUUID().toString(),
                node,
                standardType(type),
                subject,
                Instant.ofEpochMilli(millis),
                data.build());
    }

    private static String standardType(String type) {
        return switch (type) {
            case "BINDING_ADDED", "ADDRESS_ADDED" -> EventTypes.RESOURCE_CREATED;
            case "BINDING_REMOVED", "ADDRESS_REMOVED" -> EventTypes.RESOURCE_DELETED;
            case "CONNECTION_CREATED" -> EventTypes.CLIENT_CONNECTED;
            case "CONNECTION_DESTROYED" -> EventTypes.CLIENT_DISCONNECTED;
            case "SESSION_CREATED" -> EventTypes.CHANNEL_OPENED;
            case "SESSION_CLOSED" -> EventTypes.CHANNEL_CLOSED;
            case "CONSUMER_CREATED" -> EventTypes.CONSUMER_ATTACHED;
            case "CONSUMER_CLOSED" -> EventTypes.CONSUMER_DETACHED;
            case "MESSAGE_DELIVERED" -> EventTypes.MESSAGE_DELIVERED;
            case "MESSAGE_EXPIRED" -> EventTypes.MESSAGE_EXPIRED;
            default -> "artemis." + (type.isEmpty() ? "unknown" : type.toLowerCase(Locale.ROOT));
        };
    }

    private static Optional<String> text(@Nullable Object value) {
        return value == null || value.toString().isBlank() ? Optional.empty() : Optional.of(value.toString());
    }

    private static Optional<ResourceId> id(KindId kind, @Nullable Object name) {
        return text(name).map(n -> new ResourceId(kind, ResourcePath.of(n)));
    }
}
