package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.model.Attributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;

/**
 * Bytes and offset lag of each replica, from {@code describeLogDirs}. {@code ReplicaInfo.size()} is the bytes on that
 * broker and does not include remote storage. A future replica is the log a directory move has not yet swapped in, so
 * it is flagged and not used as the current log.
 */
final class ReplicaLogs {

    private ReplicaLogs() {}

    /** The size and lag of one replica. A future replica is flagged and still reports its own bytes and lag. */
    static Attributes attributes(ReplicaInfo info) {
        Attributes.Builder attributes = Attributes.builder()
                .putRaw(KafkaKinds.REPLICA_SIZE_BYTES, info.size())
                .putRaw(KafkaKinds.OFFSET_LAG, info.offsetLag());
        if (info.isFuture()) {
            attributes.putRaw(KafkaKinds.FUTURE_REPLICA, true);
        }
        return attributes.build();
    }

    /** One broker's current logs, and the future logs a directory move has not swapped in. */
    record Broker(Map<TopicPartition, ReplicaInfo> current, Map<TopicPartition, ReplicaInfo> future) {

        Broker {
            current = Map.copyOf(current);
            future = Map.copyOf(future);
        }
    }

    /**
     * What the brokers answered. {@code unavailable} names each broker that did not, with the error; it is empty when
     * every broker answered. A future replica is kept apart from the current log.
     */
    record Read(Map<Integer, Broker> brokers, String unavailable) {

        Read {
            brokers = Map.copyOf(brokers);
        }

        @Nullable ReplicaInfo current(int broker, TopicPartition partition) {
            Broker found = brokers.get(broker);
            return found == null ? null : found.current().get(partition);
        }

        @Nullable ReplicaInfo future(int broker, TopicPartition partition) {
            Broker found = brokers.get(broker);
            return found == null ? null : found.future().get(partition);
        }
    }

    /**
     * Reads each broker's own future. One broker that fails is named and left out; the brokers that answered are
     * still returned.
     */
    static Read read(Await await, Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> descriptions) {
        List<Integer> ids = new ArrayList<>(descriptions.keySet());
        ids.sort(Integer::compareTo);
        Map<Integer, Broker> brokers = new HashMap<>();
        List<String> failed = new ArrayList<>();
        for (int id : ids) {
            KafkaFuture<Map<String, LogDirDescription>> future = descriptions.get(id);
            if (future == null) {
                failed.add("broker " + id + ": no description");
                continue;
            }
            Map<String, LogDirDescription> dirs;
            try {
                dirs = await.get(future);
            } catch (BrokerException e) {
                failed.add("broker " + id + ": "
                        + (e.getMessage() == null ? e.status().name() : e.getMessage()));
                continue;
            }
            Map<TopicPartition, ReplicaInfo> current = new HashMap<>();
            Map<TopicPartition, ReplicaInfo> futureLogs = new HashMap<>();
            for (Map.Entry<String, LogDirDescription> dir : dirs.entrySet()) {
                LogDirDescription description = dir.getValue();
                if (description.error() != null) {
                    String message = description.error().getMessage();
                    failed.add("broker " + id + ": "
                            + (message == null ? description.error().getClass().getSimpleName() : message));
                    continue;
                }
                description.replicaInfos().forEach((partition, info) -> {
                    if (info.isFuture()) {
                        futureLogs.putIfAbsent(partition, info);
                    } else {
                        current.putIfAbsent(partition, info);
                    }
                });
            }
            brokers.put(id, new Broker(current, futureLogs));
        }
        return new Read(brokers, String.join("; ", failed));
    }

    /** The leader replica's bytes on that broker, ignoring a future log and a broker that did not answer. */
    static OptionalLong leaderSize(Read read, @Nullable Node leader, TopicPartition partition) {
        if (leader == null) {
            return OptionalLong.empty();
        }
        ReplicaInfo info = read.current(leader.id(), partition);
        return info == null ? OptionalLong.empty() : OptionalLong.of(info.size());
    }
}
