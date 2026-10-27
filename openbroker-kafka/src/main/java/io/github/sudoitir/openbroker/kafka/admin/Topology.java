package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Status;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.jspecify.annotations.Nullable;

/**
 * One bounded description of the cluster's topics and their partitions, shared by node health, the
 * partition kind and the polling watch, and kept for the watch's poll interval. Reading it costs a topic listing
 * and one description request, so the three readers pay once per interval between them, however
 * often they ask. A change this driver makes drops it with {@link #invalidate()}.
 */
public final class Topology {

    /** How many topics one description covers; a cluster with more is described by its first ones. */
    public static final int MAX_TOPICS = 2_000;

    private final Clock clock;
    private final Supplier<Duration> ttl;
    private @Nullable Snapshot current;

    Topology(Clock clock, Supplier<Duration> ttl) {
        this.clock = clock;
        this.ttl = ttl;
    }

    /** What one reading found. */
    public record Snapshot(Instant takenAt, List<TopicDescription> topics, int totalTopics) {

        public Snapshot {
            topics = List.copyOf(topics);
        }

        /** Whether the description covers every topic of the cluster. */
        public boolean complete() {
            return topics.size() == totalTopics;
        }
    }

    /** The description of the topics, as old as the watch interval at most. */
    public synchronized Snapshot read(Admin admin, Await await) {
        Snapshot held = current;
        if (held != null && Duration.between(held.takenAt(), clock.instant()).compareTo(ttl.get()) < 0) {
            return held;
        }
        Snapshot fresh = describe(admin, await);
        current = fresh;
        return fresh;
    }

    /** Forgets the description, because this driver changed what it describes. */
    public synchronized void invalidate() {
        current = null;
    }

    private Snapshot describe(Admin admin, Await await) {
        Set<String> all = new TreeSet<>(await.get(
                admin.listTopics(new ListTopicsOptions().listInternal(true).timeoutMs(await.timeoutMs()))
                        .names()));
        List<String> names = new ArrayList<>(all).subList(0, Math.min(all.size(), MAX_TOPICS));
        if (names.isEmpty()) {
            return new Snapshot(clock.instant(), List.of(), 0);
        }
        Map<String, KafkaFuture<TopicDescription>> pending = admin.describeTopics(
                        TopicCollection.ofTopicNames(names), new DescribeTopicsOptions().timeoutMs(await.timeoutMs()))
                .topicNameValues();
        List<TopicDescription> topics = new ArrayList<>();
        for (String name : names) {
            try {
                topics.add(await.get(pending.get(name)));
            } catch (BrokerException e) {
                // A topic deleted since it was listed is no longer there to describe.
                if (e.status() != Status.NOT_FOUND) {
                    throw e;
                }
            }
        }
        return new Snapshot(clock.instant(), topics, all.size());
    }
}
