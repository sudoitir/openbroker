package io.github.sudoitir.openbroker.rabbitmq.messaging;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownSignalException;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.BrowsePage;
import io.github.sudoitir.openbroker.messaging.BrowseRequest;
import io.github.sudoitir.openbroker.messaging.BulkOutcome;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.messaging.Denominator;
import io.github.sudoitir.openbroker.messaging.MessageAction;
import io.github.sudoitir.openbroker.messaging.MessageField;
import io.github.sudoitir.openbroker.messaging.MessageFilter;
import io.github.sudoitir.openbroker.messaging.Messages;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.Selection;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Total;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqKinds;
import io.github.sudoitir.openbroker.rabbitmq.amqp.AmqpCalls;
import io.github.sudoitir.openbroker.rabbitmq.amqp.AmqpConnections;
import io.github.sudoitir.openbroker.rabbitmq.amqp.MessageCodec;
import io.github.sudoitir.openbroker.rabbitmq.amqp.PooledAmqpConnections;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import tools.jackson.databind.JsonNode;

/**
 * {@link Messages}: browse (streams only), count, send, and the {@code UNIMPLEMENTED} count by a field and bulk
 * actions.
 *
 * <p>A browse reads a stream through {@link StreamReader}; a queue is refused before any channel or request, because
 * RabbitMQ gives a queue's messages only to a consumer, which takes them. A send publishes one message on a channel in
 * confirm mode opened for it and waits for the confirm within the call's deadline.
 */
public final class RabbitMqMessages implements Messages, AutoCloseable {

    private static final String DEFAULT_EXCHANGE_ID = "amq.default";

    private static final String NO_FIELD_COUNT =
            "RabbitMQ cannot count by a field without taking the messages off the queue.";

    private static final String NO_FILTER_COUNT = "RabbitMQ cannot count by a filter without taking messages.";

    private static final String NO_BULK =
            "RabbitMQ cannot move, copy or delete single messages: a queue gives its messages only to a consumer, which"
                    + " takes them, and a stream keeps its messages until its retention drops them.";

    private final RabbitMqCluster cluster;
    private final AmqpConnections connections;
    private final ConcurrentMap<ResourceId, StreamReader.Known> firstOffsets = new ConcurrentHashMap<>();

    public RabbitMqMessages(RabbitMqCluster cluster) {
        this(cluster, new PooledAmqpConnections(cluster));
    }

    RabbitMqMessages(RabbitMqCluster cluster, AmqpConnections connections) {
        this.cluster = cluster;
        this.connections = connections;
    }

    @Override
    public BrowsePage browse(ResourceId subscription, Scope scope, BrowseRequest request) {
        StreamReader.requireStream(subscription);
        return reader(subscription).browse(request);
    }

    private StreamReader reader(ResourceId stream) {
        return new StreamReader(
                cluster, stream, connections, false, ReceiverOptions.browse(Optional.empty()), firstOffsets);
    }

    @Override
    public Partial<Total> count(ResourceId subscription, Scope scope, Optional<MessageFilter> filter) {
        if (filter.isPresent()) {
            throw StreamReader.refused(NO_FILTER_COUNT);
        }
        String kind = subscription.kind().value();
        boolean queue = RabbitMqKinds.CLASSIC_QUEUE.value().equals(kind)
                || RabbitMqKinds.QUORUM_QUEUE.value().equals(kind);
        if (!queue && !RabbitMqKinds.STREAM.value().equals(kind)) {
            throw RabbitMqErrors.invalid("Messages are in a queue or a stream, not in " + kind + ".");
        }
        if (subscription.path().segments().size() != 2) {
            throw RabbitMqErrors.invalid("A queue or a stream is named by its virtual host and its name.");
        }
        Deadline deadline = Deadlines.current(cluster.options());
        Total total = cluster.call(
                "messages.count",
                Optional.of(subscription),
                deadline,
                () -> queue
                        ? queueTotal(subscription, deadline)
                        : reader(subscription).total(deadline));
        return Partial.complete(total, List.of(RabbitMqCluster.CLUSTER));
    }

    /**
     * The ready messages of a queue: exact from a passive declare when nothing is unacknowledged, an estimate when
     * something is or when management cannot say, and management's sampled total when there is no AMQP endpoint.
     */
    private Total queueTotal(ResourceId queue, Deadline deadline) {
        String vhost = queue.path().segments().get(0);
        String name = queue.name();
        if (!hasDataEndpoint(cluster.spec())) {
            JsonNode info = queueInfo(cluster, vhost, name, deadline, "messages.count");
            return new Total(
                    Denominator.ALL, Count.estimate(info.path("messages").asLong(0)));
        }
        long ready = new AmqpCalls(connections, cluster.options().clock())
                .queueDeclarePassive(vhost, name, deadline)
                .getMessageCount();
        long unacknowledged;
        try {
            unacknowledged = queueInfo(cluster, vhost, name, deadline, "messages.count")
                    .path("messages_unacknowledged")
                    .asLong(0);
        } catch (BrokerException e) {
            // Management cannot say what is unacknowledged, so the ready count is only an estimate.
            return new Total(Denominator.ALL, Count.estimate(ready));
        }
        return new Total(
                Denominator.ALL, unacknowledged == 0 ? Count.exact(ready) : Count.estimate(ready + unacknowledged));
    }

    private static boolean hasDataEndpoint(ClusterSpec spec) {
        return spec.clusterEndpoints().stream().anyMatch(RabbitMqMessages::isData)
                || spec.nodes().stream()
                        .map(NodeSpec::endpoints)
                        .flatMap(List::stream)
                        .anyMatch(RabbitMqMessages::isData);
    }

    private static boolean isData(Endpoint endpoint) {
        return endpoint.plane() == Plane.DATA;
    }

    /** The management API's JSON for the queue or stream {@code name} of {@code vhost}. */
    static JsonNode queueInfo(RabbitMqCluster cluster, String vhost, String name, Deadline deadline, String operation) {
        return cluster.management()
                .get("/api/queues/" + segment(vhost) + "/" + segment(name), Map.of(), deadline, operation)
                .body();
    }

    private static String segment(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    @Override
    public Partial<Map<String, Long>> countBy(ResourceId subscription, Scope scope, MessageField field) {
        throw StreamReader.refused(NO_FIELD_COUNT);
    }

    /**
     * Publishes one message with the mandatory flag on a channel in confirm mode, after a passive declare of its
     * target, and waits for the confirm within the call's deadline. A message routed nowhere fails
     * {@code NOT_ROUTED}; a negative confirm fails {@code DESTINATION_FULL}.
     */
    @Override
    public SendReceipt send(SendTarget target, Scope scope, OutboundMessage message) {
        Deadline deadline = Deadlines.current(cluster.options());
        ResourceId destination;
        String exchange;
        String routingKey;
        switch (target) {
            case SendTarget.ToSubscription to -> {
                destination = to.subscription();
                exchange = "";
                routingKey = to.subscription().name();
            }
            case SendTarget.ToDestination to -> {
                if (to.partition().isPresent()) {
                    throw RabbitMqErrors.invalid("A RabbitMQ destination has no partitions to send to.");
                }
                destination = to.destination();
                exchange = DEFAULT_EXCHANGE_ID.equals(destination.name()) ? "" : destination.name();
                routingKey = to.routingKey().orElse("");
            }
        }
        if (destination.path().segments().size() != 2) {
            throw RabbitMqErrors.invalid("A queue, a stream or an exchange is named by its virtual host and its name.");
        }
        String vhost = destination.path().segments().get(0);
        boolean isExchange =
                RabbitMqKinds.EXCHANGE.value().equals(destination.kind().value());
        boolean isQueue = isQueue(destination.kind().value());
        if (target instanceof SendTarget.ToSubscription ? !isQueue : !(isExchange || isQueue)) {
            throw RabbitMqErrors.invalid(
                    "Messages are sent to an exchange, or to a queue or a stream, not to " + destination.kind() + ".");
        }
        AMQP.BasicProperties properties = MessageCodec.properties(message);
        byte[] body = MessageCodec.body(message);
        return cluster.call("messages.send", Optional.of(destination), deadline, () -> {
            AmqpCalls calls = new AmqpCalls(connections, cluster.options().clock());
            if (isExchange) {
                if (!exchange.isEmpty()) {
                    calls.exchangeDeclarePassive(vhost, exchange, deadline);
                }
            } else {
                calls.queueDeclarePassive(vhost, destination.name(), deadline);
            }
            publish(vhost, exchange, routingKey, properties, body, destination, deadline);
            return new SendReceipt(message.headers().id(), Optional.empty(), Optional.empty(), false);
        });
    }

    private static boolean isQueue(String kind) {
        return RabbitMqKinds.CLASSIC_QUEUE.value().equals(kind)
                || RabbitMqKinds.QUORUM_QUEUE.value().equals(kind)
                || RabbitMqKinds.STREAM.value().equals(kind);
    }

    private void publish(
            String vhost,
            String exchange,
            String routingKey,
            AMQP.BasicProperties properties,
            byte[] body,
            ResourceId destination,
            Deadline deadline) {
        Connection connection = connections.connection(vhost, deadline);
        Channel channel;
        try {
            channel = connection.createChannel();
        } catch (IOException e) {
            throw RabbitMqErrors.fromIo(e, false);
        }
        AtomicBoolean returned = new AtomicBoolean();
        try {
            channel.confirmSelect();
            channel.addReturnListener(r -> returned.set(true));
            channel.basicPublish(exchange, routingKey, true, properties, body);
            long millis =
                    Math.max(1, deadline.remaining(cluster.options().clock()).toMillis());
            if (!channel.waitForConfirms(millis)) {
                throw RabbitMqErrors.nacked(Optional.of(destination));
            }
        } catch (IOException e) {
            throw RabbitMqErrors.fromIo(e, true);
        } catch (TimeoutException e) {
            throw Deadlines.exceeded(RabbitMqCluster.CLUSTER, Dispatch.IN_DOUBT);
        } catch (InterruptedException e) {
            throw Deadlines.cancelled(RabbitMqCluster.CLUSTER, e);
        } finally {
            closeQuietly(channel);
        }
        if (returned.get()) {
            throw new BrokerException(
                    Status.FAILED_PRECONDITION,
                    ErrorInfo.of(ErrorReasons.NOT_ROUTED, Map.of(ErrorReasons.RESOURCE, destination.format())),
                    Dispatch.REJECTED,
                    null,
                    "RabbitMQ took the message and no queue received it: the exchange has no binding for the routing key.");
        }
    }

    private static void closeQuietly(Channel channel) {
        try {
            if (channel.isOpen()) {
                channel.close();
            }
        } catch (IOException | TimeoutException | ShutdownSignalException e) {
            // The channel is gone either way.
        }
    }

    @Override
    public Partial<BulkOutcome> bulk(ResourceId subscription, Scope scope, Selection selection, MessageAction action) {
        throw StreamReader.refused(NO_BULK);
    }

    @Override
    public void close() {
        connections.close();
    }
}
