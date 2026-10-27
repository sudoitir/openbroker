package io.github.sudoitir.openbroker.kafka.admin;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;

/** Log offsets of partitions: where they start, where they end, and where a time falls. */
final class Offsets {

    private Offsets() {}

    /** The partitions of a topic. */
    static List<TopicPartition> partitions(Context context, String topic) {
        TopicDescription description = context.await()
                .get(context.admin()
                        .describeTopics(
                                TopicCollection.ofTopicNames(List.of(topic)),
                                new DescribeTopicsOptions()
                                        .timeoutMs(context.await().timeoutMs()))
                        .topicNameValues()
                        .get(topic));
        return description.partitions().stream()
                .map(p -> new TopicPartition(topic, p.partition()))
                .toList();
    }

    static Map<TopicPartition, Long> earliest(Context context, Collection<TopicPartition> partitions) {
        return list(context, partitions, OffsetSpec.earliest());
    }

    static Map<TopicPartition, Long> latest(Context context, Collection<TopicPartition> partitions) {
        return list(context, partitions, OffsetSpec.latest());
    }

    /** The first offset at or after {@code epochMillis} on each partition, or the end where none is. */
    static Map<TopicPartition, Long> atTime(Context context, Collection<TopicPartition> partitions, long epochMillis) {
        Map<TopicPartition, Long> found = list(context, partitions, OffsetSpec.forTimestamp(epochMillis));
        List<TopicPartition> none = found.entrySet().stream()
                .filter(e -> e.getValue() < 0)
                .map(Map.Entry::getKey)
                .toList();
        if (!none.isEmpty()) {
            found.putAll(latest(context, none));
        }
        return found;
    }

    private static Map<TopicPartition, Long> list(
            Context context, Collection<TopicPartition> partitions, OffsetSpec spec) {
        if (partitions.isEmpty()) {
            return Map.of();
        }
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        partitions.forEach(p -> request.put(p, spec));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> found = context.await()
                .get(context.admin()
                        .listOffsets(
                                request,
                                // Committed records only: what a read-committed group can still reach, so a
                                // transaction that is open or aborted is not counted as waiting.
                                new ListOffsetsOptions(IsolationLevel.READ_COMMITTED)
                                        .timeoutMs(context.await().timeoutMs()))
                        .all());
        Map<TopicPartition, Long> out =
                new TreeMap<>(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition));
        found.forEach((p, info) -> out.put(p, info.offset()));
        return out;
    }
}
