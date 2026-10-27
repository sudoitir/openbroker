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

/**
 * Tells a subscriber when a consumer attaches to a subscription or leaves it: every {@code
 * watch.interval} it reads the members of the consumer, classic and share groups, and reports the
 * (member, topic) pairs that appeared and disappeared. A member shows on the topics it is assigned.
 * The first read only learns who is there, and a read that fails is skipped.
 */
final class MemberWatch implements AutoCloseable {

    private static final int BUFFER = 1024;

    private final KafkaCluster cluster;
    private final Handlers handlers;
    private final NotificationDispatcher dispatcher;
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("openbroker-kafka-members").factory());
    private Map<String, Attachment> known;

    private record Attachment(ResourceId subscription, String clientId, String host) {}

    MemberWatch(KafkaCluster cluster, Handlers handlers, NotificationListener listener) {
        this.cluster = cluster;
        this.handlers = handlers;
        this.dispatcher =
                new NotificationDispatcher(listener, BUFFER, cluster.options().clock(), r -> r);
        timer.scheduleWithFixedDelay(this::cycle, 0, cluster.watchInterval().toMillis(), TimeUnit.MILLISECONDS);
    }

    private void cycle() {
        Map<String, Attachment> now;
        try {
            now = read();
        } catch (BrokerException e) {
            return;
        }
        Instant at = cluster.options().clock().instant();
        if (known != null) {
            now.forEach((key, a) -> {
                if (!known.containsKey(key)) {
                    emit(EventTypes.CONSUMER_ATTACHED, a, at);
                }
            });
            known.forEach((key, a) -> {
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
