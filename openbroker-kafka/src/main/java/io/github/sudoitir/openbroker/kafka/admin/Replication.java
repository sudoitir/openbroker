package io.github.sudoitir.openbroker.kafka.admin;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

/** How replicated the partitions are. */
final class Replication {

    private Replication() {}

    /**
     * For each broker, how many partitions list it as a replica that is not in sync: it holds a copy
     * but is not in the in-sync set, so a leader loss could cost data or availability.
     */
    static Map<Integer, Long> outOfSyncPartitions(Collection<TopicDescription> topics) {
        Map<Integer, Long> out = new HashMap<>();
        for (TopicDescription topic : topics) {
            for (TopicPartitionInfo partition : topic.partitions()) {
                Set<Integer> inSync = new HashSet<>();
                partition.isr().forEach(n -> inSync.add(n.id()));
                for (Node replica : partition.replicas()) {
                    if (!inSync.contains(replica.id())) {
                        out.merge(replica.id(), 1L, Long::sum);
                    }
                }
            }
        }
        return out;
    }
}
