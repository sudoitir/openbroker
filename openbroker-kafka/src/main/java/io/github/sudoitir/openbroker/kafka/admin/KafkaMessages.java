package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorReasons;
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
import io.github.sudoitir.openbroker.messaging.MessageRef;
import io.github.sudoitir.openbroker.messaging.Messages;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.Selection;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Total;
import io.github.sudoitir.openbroker.model.MetricKeys;
import io.github.sudoitir.openbroker.model.PartitionId;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.Shard;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.jspecify.annotations.Nullable;

/**
 * Browsing, counting and sending by hand. A browse never joins a group; a count of a subscription is
 * its lag, an estimate; a send is one record, written and acknowledged by every in-sync replica.
 * Counting by a filter, and moving, copying or deleting a single message, are not offered: Kafka's log
 * is read in order and removed from its start, so each would need every record read or rewritten.
 */
public final class KafkaMessages implements Messages, AutoCloseable {

    /** Why Kafka offers no per-message action. */
    static final String LOG_IS_APPEND_ONLY =
            "Kafka's log is append-only: a single record cannot be deleted, and moving or copying one means writing"
                    + " it again at a new offset, which would lie about the log. Truncate from the start of the log"
                    + " instead, or produce the record again yourself.";

    private final KafkaCluster cluster;
    private final Handlers handlers;
    private @Nullable KafkaProducer<byte[], byte[]> producer;

    KafkaMessages(KafkaCluster cluster, Handlers handlers) {
        this.cluster = cluster;
        this.handlers = handlers;
    }

    /** The topic a resource names, and the partition if it names one. */
    private record Place(String topic, Optional<Integer> partition) {}

    private static Place place(ResourceId id) {
        String kind = id.kind().value();
        return switch (kind) {
            case "kafka.topic", "kafka.group-topic", "kafka.share-group-topic" ->
                new Place(id.name(), Optional.empty());
            case "kafka.partition" -> {
                TopicPartition tp = Partitions.partition(id);
                yield new Place(tp.topic(), Optional.of(tp.partition()));
            }
            default ->
                throw Attrs.invalid(
                        "Messages are in a topic, a partition, or a group's subscription to a topic, not in "
                                + id.kind() + ".");
        };
    }

    // Browse and count.

    @Override
    public BrowsePage browse(ResourceId subscription, Scope scope, BrowseRequest request) {
        Deadline deadline = Deadlines.current(cluster.options());
        Place place = place(subscription);
        Scope narrowed = place.partition().isPresent() ? partitionScope(place) : scope;
        return handlers.callAs(
                "messages.browse",
                Plane.DATA,
                Optional.of(subscription),
                deadline,
                context -> Browsing.browse(context, subscription, place.topic(), narrowed, request, deadline));
    }

    private static Scope partitionScope(Place place) {
        return Scope.shards(Set.of(
                Shard.partition(PartitionId.of(String.valueOf(place.partition().orElseThrow())))));
    }

    @Override
    public Partial<Total> count(ResourceId subscription, Scope scope, Optional<MessageFilter> filter) {
        Deadline deadline = Deadlines.current(cluster.options());
        return Outcomes.of(new Total(Denominator.ALL, Count.unavailable("The count could not be read.")), () -> {
            if (filter.isPresent()) {
                throw KafkaErrors.refused(
                        Status.UNIMPLEMENTED,
                        ErrorReasons.NOT_SUPPORTED,
                        "Kafka cannot count by a filter without reading every record.");
            }
            Place place = place(subscription);
            return handlers.callAs("messages.count", Plane.MANAGEMENT, Optional.of(subscription), deadline, context -> {
                if (subscription.kind().value().endsWith("group-topic")) {
                    Resource resource = handlers.of(context, subscription.kind())
                            .find(context, subscription)
                            .orElseThrow(() -> Handler.gone(subscription));
                    long backlog = resource.attributes()
                            .get(MetricKeys.BACKLOG)
                            .orElseThrow()
                            .asLong();
                    return new Total(Denominator.ALL, Count.estimate(backlog));
                }
                List<TopicPartition> partitions = Offsets.partitions(context, place.topic()).stream()
                        .filter(tp ->
                                place.partition().map(p -> p == tp.partition()).orElse(true))
                        .toList();
                return new Total(Denominator.ALL, Browsing.size(context, Seeks.partitionsIn(scope, partitions)));
            });
        });
    }

    @Override
    public Partial<Map<String, Long>> countBy(ResourceId subscription, Scope scope, MessageField field) {
        return Outcomes.of(Map.of(), () -> {
            throw KafkaErrors.refused(
                    Status.UNIMPLEMENTED,
                    ErrorReasons.NOT_SUPPORTED,
                    "Kafka cannot count by a field without reading every record.");
        });
    }

    @Override
    public Partial<BulkOutcome> bulk(ResourceId subscription, Scope scope, Selection selection, MessageAction action) {
        throw KafkaErrors.refused(Status.UNIMPLEMENTED, ErrorReasons.NOT_SUPPORTED, LOG_IS_APPEND_ONLY);
    }

    // Send.

    @Override
    public SendReceipt send(SendTarget target, Scope scope, OutboundMessage message) {
        Deadline deadline = Deadlines.current(cluster.options());
        ResourceId destination;
        Optional<String> routingKey = Optional.empty();
        Optional<Integer> partition = Optional.empty();
        switch (target) {
            case SendTarget.ToSubscription to -> destination = to.subscription();
            case SendTarget.ToDestination to -> {
                destination = to.destination();
                routingKey = to.routingKey();
                partition = to.partition().map(p -> Integer.parseInt(p.value()));
            }
        }
        Place place = place(destination);
        Optional<Integer> chosen = place.partition().isPresent() ? place.partition() : partition;
        ProducerRecord<byte[], byte[]> record = RecordCodec.record(place.topic(), chosen, routingKey, message);
        ResourceId sentTo = destination;
        return handlers.callAs("messages.send", Plane.DATA, Optional.of(destination), deadline, context -> {
            // A send must not make the topic: the broker would, with its defaults, for a name misspelled.
            Offsets.partitions(context, place.topic());
            RecordMetadata sent = produce(record, deadline);
            return new SendReceipt(
                    message.headers().id(),
                    Optional.of(new MessageRef(
                            sentTo,
                            Shard.partition(PartitionId.of(String.valueOf(sent.partition()))),
                            String.valueOf(sent.offset()))),
                    Optional.empty(),
                    false);
        });
    }

    private RecordMetadata produce(ProducerRecord<byte[], byte[]> record, Deadline deadline) {
        try {
            return producer()
                    .send(record)
                    .get(
                            Math.max(
                                    1,
                                    deadline.remaining(cluster.options().clock())
                                            .toMillis()),
                            TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw KafkaErrors.of(e.getCause() == null ? e : e.getCause(), KafkaCluster.CLUSTER);
        } catch (TimeoutException e) {
            throw Deadlines.exceeded(KafkaCluster.CLUSTER, Dispatch.IN_DOUBT);
        } catch (InterruptedException e) {
            throw Deadlines.cancelled(KafkaCluster.CLUSTER, e);
        }
    }

    private synchronized KafkaProducer<byte[], byte[]> producer() {
        if (producer == null) {
            Map<String, Object> config = new HashMap<>(cluster.clientConfig(Plane.DATA));
            long call = cluster.options().timeouts().call().toMillis();
            config.put(ProducerConfig.ACKS_CONFIG, "all");
            config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            config.put(ProducerConfig.LINGER_MS_CONFIG, 0);
            config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, call);
            config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) Math.min(call, Integer.MAX_VALUE));
            config.put(ProducerConfig.CLIENT_ID_CONFIG, config.get(ProducerConfig.CLIENT_ID_CONFIG) + "-send");
            producer = new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());
        }
        return producer;
    }

    /** Closes the producer; the next send makes another from the connection's settings. */
    synchronized void reset() {
        if (producer != null) {
            producer.close(Duration.ofSeconds(2));
            producer = null;
        }
    }

    @Override
    public void close() {
        reset();
    }
}
