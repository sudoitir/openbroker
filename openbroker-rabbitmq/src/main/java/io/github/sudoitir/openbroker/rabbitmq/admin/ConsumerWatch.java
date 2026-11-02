package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import io.github.sudoitir.openbroker.notification.BrokerEvent;
import io.github.sudoitir.openbroker.notification.EventTypes;
import io.github.sudoitir.openbroker.notification.NotificationListener;
import io.github.sudoitir.openbroker.rabbitmq.RabbitMqOptions;
import io.github.sudoitir.openbroker.support.Deadlines;
import io.github.sudoitir.openbroker.support.NotificationDispatcher;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;

/**
 * Polls {@code /api/consumers} every {@code rabbitmq.watch_interval} and emits {@code CONSUMER_ATTACHED} and
 * {@code CONSUMER_DETACHED} with the subscription attribute, beside the polling notifications of resources. The first
 * read, made by {@link #start()}, only learns who is there, so a consumer that attaches afterwards is always seen as
 * new; a read that fails is retried at once a few times there, and skipped later.
 */
final class ConsumerWatch implements AutoCloseable {

    private static final int BUFFER = 1024;
    private static final int BASELINE_ATTEMPTS = 3;
    private static final String UNKNOWN = "unknown";

    private final RabbitMqCluster cluster;
    private final NotificationDispatcher dispatcher;
    private final ScheduledExecutorService timer;
    private @Nullable Map<String, Attachment> known;

    private record Attachment(ResourceId subscription, ResourceId consumer, ResourceId channel) {}

    ConsumerWatch(RabbitMqCluster cluster, NotificationListener listener) {
        this(
                cluster,
                listener,
                Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform()
                        .daemon()
                        .name("openbroker-rabbitmq-consumers")
                        .factory()));
    }

    /** A watch whose reads {@code timer} schedules, so a test drives them one at a time. */
    ConsumerWatch(RabbitMqCluster cluster, NotificationListener listener, ScheduledExecutorService timer) {
        this.cluster = cluster;
        this.timer = timer;
        this.dispatcher =
                new NotificationDispatcher(listener, BUFFER, cluster.options().clock(), r -> r);
    }

    /** Learns who is attached now, then starts reading at every interval. */
    void start() {
        this.known = baseline();
        long interval = RabbitMqOptions.watchInterval(cluster.spec()).toMillis();
        timer.scheduleWithFixedDelay(this::cycle, interval, interval, TimeUnit.MILLISECONDS);
    }

    private @Nullable Map<String, Attachment> baseline() {
        for (int attempt = 1; attempt <= BASELINE_ATTEMPTS; attempt++) {
            try {
                return read();
            } catch (BrokerException _) {
                // A consumer listing that failed once may answer the next time; the first cycle that can read takes it.
            }
        }
        return null;
    }

    /** One read and its diff against the one before. */
    void cycle() {
        Map<String, Attachment> now;
        try {
            now = read();
        } catch (BrokerException _) {
            return; // The next cycle reads again; what changed meanwhile shows then.
        }
        Instant at = cluster.options().clock().instant();
        Map<String, Attachment> before = known;
        if (before != null) {
            now.forEach((key, a) -> {
                if (!before.containsKey(key)) {
                    emit(EventTypes.CONSUMER_ATTACHED, a, at);
                }
            });
            before.forEach((key, a) -> {
                if (!now.containsKey(key)) {
                    emit(EventTypes.CONSUMER_DETACHED, a, at);
                }
            });
        }
        known = now;
    }

    private Map<String, Attachment> read() {
        JsonNode body = cluster.management()
                .get("/api/consumers", Map.of(), Deadlines.current(cluster.options()), "list the consumers")
                .body();
        Map<String, Attachment> out = new HashMap<>();
        for (JsonNode item : body) {
            String vhost = text(item.path("queue").path("vhost"));
            String queue = text(item.path("queue").path("name"));
            String channel = text(item.path("channel_details").path("name"));
            String tag = text(item.path("consumer_tag"));
            ResourceId subscription =
                    new ResourceId(cluster.queueTypes().kindOf(vhost, queue), ResourcePath.of(vhost, queue));
            ResourceId consumer = new ResourceId(RabbitMqKinds.CONSUMER, ResourcePath.of(vhost, channel, tag));
            ResourceId channelId = new ResourceId(RabbitMqKinds.CHANNEL, ResourcePath.of(channel));
            out.put(vhost + "\u0000" + channel + "\u0000" + tag, new Attachment(subscription, consumer, channelId));
        }
        return out;
    }

    private static String text(JsonNode node) {
        return node.isString() && !node.asString().isEmpty() ? node.asString() : UNKNOWN;
    }

    private void emit(String type, Attachment a, Instant at) {
        Attributes data = Attributes.builder()
                .put(StandardAttributes.SUBSCRIPTION, a.subscription())
                .put(StandardAttributes.CONSUMER, a.consumer())
                .put(StandardAttributes.CHANNEL, a.channel())
                .build();
        dispatcher.event(new BrokerEvent(
                UUID.randomUUID().toString(), RabbitMqCluster.CLUSTER, type, Optional.of(a.subscription()), at, data));
    }

    @Override
    public void close() {
        timer.shutdownNow();
        dispatcher.close();
    }
}
