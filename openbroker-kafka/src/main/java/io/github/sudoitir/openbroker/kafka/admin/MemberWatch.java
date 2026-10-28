package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import io.github.sudoitir.openbroker.notification.BrokerEvent;
import io.github.sudoitir.openbroker.notification.EventTypes;
import io.github.sudoitir.openbroker.notification.NotificationListener;
import io.github.sudoitir.openbroker.support.Deadlines;
import io.github.sudoitir.openbroker.support.NotificationDispatcher;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.common.GroupType;
import org.jspecify.annotations.Nullable;

/**
 * Tells a subscriber when a consumer attaches to a subscription or leaves it: every {@code
 * watch.interval} it reads the members of the consumer, classic and share groups, and reports the
 * (member, topic) pairs that appeared and disappeared. A member shows on the topics it is assigned.
 * The first read only learns who is there. It is made before the subscription is handed back, so a
 * consumer that attaches afterwards is always seen as new; a read that fails is retried at once a
 * few times there, and skipped later.
 */
final class MemberWatch implements AutoCloseable {

    private static final int BUFFER = 1024;
    private static final int BASELINE_ATTEMPTS = 3;

    private final KafkaCluster cluster;
    private final Handlers handlers;
    private final NotificationDispatcher dispatcher;
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("openbroker-kafka-members").factory());
    private @Nullable Map<String, Attachment> known;

    private record Attachment(ResourceId subscription, String clientId, String host) {}

    MemberWatch(KafkaCluster cluster, Handlers handlers, NotificationListener listener) {
        this.cluster = cluster;
        this.handlers = handlers;
        this.dispatcher =
                new NotificationDispatcher(listener, BUFFER, cluster.options().clock(), r -> r);
        this.known = baseline();
        long interval = cluster.watchInterval().toMillis();
        timer.scheduleWithFixedDelay(this::cycle, interval, interval, TimeUnit.MILLISECONDS);
    }

    /**
     * Who is attached now, read before the caller can attach anything new; null if the cluster would not
     * say, in which case the first cycle that can read takes it.
     */
    private @Nullable Map<String, Attachment> baseline() {
        for (int attempt = 1; attempt <= BASELINE_ATTEMPTS; attempt++) {
            try {
                return read();
            } catch (BrokerException _) {
                // A group deleted between the listing and its description fails one read, not the next.
            }
        }
        return null;
    }

    private void cycle() {
        Map<String, Attachment> now;
        try {
            now = read();
        } catch (BrokerException e) {
            return;
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
        Deadline deadline = Deadlines.current(cluster.options());
        return handlers.call("watch.members", Optional.empty(), deadline, context -> {
            List<GroupListing> listings = GroupInfo.list(context).stream()
                    .filter(g -> !GroupType.STREAMS.equals(g.type().orElse(GroupType.UNKNOWN)))
                    .toList();
            Map<String, Attachment> out = new HashMap<>();
            Set<String> seen = new HashSet<>();
            for (GroupInfo group : GroupInfo.describe(context, listings).values()) {
                String kind = GroupType.SHARE.equals(group.type())
                        ? KafkaKinds.SHARE_GROUP_TOPIC.value()
                        : KafkaKinds.GROUP_TOPIC.value();
                for (GroupInfo.Member member : group.members()) {
                    for (String topic : member.topics()) {
                        String key = group.id() + "\u0000" + member.id() + "\u0000" + topic;
                        if (seen.add(key)) {
                            out.put(
                                    key,
                                    new Attachment(
                                            new ResourceId(KindId.of(kind), ResourcePath.of(group.id(), topic)),
                                            member.clientId(),
                                            member.host()));
                        }
                    }
                }
            }
            return out;
        });
    }

    private void emit(String type, Attachment a, Instant at) {
        Attributes data = Attributes.builder()
                .put(StandardAttributes.SUBSCRIPTION, a.subscription())
                .put(StandardAttributes.CLIENT_ID, a.clientId())
                .put(StandardAttributes.REMOTE_ADDRESS, a.host())
                .build();
        dispatcher.event(new BrokerEvent(
                UUID.randomUUID().toString(), KafkaCluster.CLUSTER, type, Optional.of(a.subscription()), at, data));
    }

    @Override
    public void close() {
        timer.shutdownNow();
        dispatcher.close();
    }
}
