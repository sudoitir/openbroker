package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.messaging.Position;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.AttributeChange;
import io.github.sudoitir.openbroker.operation.Estimate;
import io.github.sudoitir.openbroker.operation.OperationResult;
import io.github.sudoitir.openbroker.operation.PreconditionEnforcement;
import io.github.sudoitir.openbroker.operation.Preview;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DeleteRecordsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListStreamsGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListStreamsGroupOffsetsSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;

/**
 * Truncate: delete a topic's records before a position, for every group. Kafka deletes from the start
 * of a partition's log and cannot keep a group's unread records, so the preview names each group that
 * would lose some and how many.
 */
final class Truncation {

    /** The attribute key of the unread records a group loses, followed by the group's name. */
    static final String UNREAD_LOST = "kafka.unread_lost.";

    private static final String CLEANUP_POLICY = "cleanup.policy";

    private Truncation() {}

    /** What a truncation would do. */
    private record Plan(
            Map<TopicPartition, Long> start, Map<TopicPartition, Long> target, Map<String, Long> unreadLost) {

        long removed() {
            return target.entrySet().stream()
                    .mapToLong(e -> e.getValue() - start.get(e.getKey()))
                    .sum();
        }
    }

    private static Plan plan(Context context, String topic, Optional<Integer> only, Scope scope, Position position) {
        requireDeletable(context, topic);
        List<TopicPartition> all = Offsets.partitions(context, topic);
        List<TopicPartition> named = all.stream()
                .filter(tp -> only.map(p -> p == tp.partition()).orElse(true))
                .toList();
        Set<TopicPartition> partitions = Seeks.partitionsIn(scope, named);
        Map<TopicPartition, Long> start = Offsets.earliest(context, partitions);
        Map<TopicPartition, Long> wanted = Seeks.resolve(context, partitions, position);
        Map<TopicPartition, Long> target = sorted();
        wanted.forEach((tp, offset) -> {
            if (offset > start.get(tp)) {
                target.put(tp, offset);
            }
        });
        return new Plan(start, target, target.isEmpty() ? Map.of() : unreadLost(context, start, target));
    }

    private static Map<TopicPartition, Long> sorted() {
        return new TreeMap<>(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition));
    }

    /** Refuses a topic that only compacts: Kafka keeps its records by key and refuses to delete them. */
    private static void requireDeletable(Context context, String topic) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Config config = context.await()
                .get(context.admin()
                        .describeConfigs(
                                List.of(resource),
                                new DescribeConfigsOptions()
                                        .timeoutMs(context.await().timeoutMs()))
                        .all())
                .get(resource);
        var policy = config.get(CLEANUP_POLICY);
        String value = policy == null || policy.value() == null ? "delete" : policy.value();
        if (!value.toLowerCase(Locale.ROOT).contains("delete")) {
            throw KafkaErrors.refused(
                    Status.FAILED_PRECONDITION,
                    ErrorReasons.PRECONDITION_FAILED,
                    "Topic " + topic + " has " + CLEANUP_POLICY + "=" + value
                            + ": it compacts and does not delete, so Kafka keeps its records by key and refuses to remove"
                            + " them. Add delete to the cleanup policy first.");
        }
    }

    /**
     * For every group, the records it has not read that the truncation removes: on each partition,
     * those from where the group stands (or the log start, if it stands before) up to the new start.
     */
    private static Map<String, Long> unreadLost(
            Context context, Map<TopicPartition, Long> start, Map<TopicPartition, Long> target) {
        Collection<TopicPartition> partitions = target.keySet();
        List<GroupListing> groups = GroupInfo.list(context);
        Map<String, Long> lost = new TreeMap<>();
        int timeout = context.await().timeoutMs();
        Map<String, ListConsumerGroupOffsetsSpec> consumers = new HashMap<>();
        Map<String, ListShareGroupOffsetsSpec> shares = new HashMap<>();
        Map<String, ListStreamsGroupOffsetsSpec> streams = new HashMap<>();
        for (GroupListing group : groups) {
            String type = group.type().orElse(GroupType.UNKNOWN).toString();
            switch (type) {
                case "Share" ->
                    shares.put(group.groupId(), new ListShareGroupOffsetsSpec().topicPartitions(partitions));
                case "Streams" ->
                    streams.put(group.groupId(), new ListStreamsGroupOffsetsSpec().topicPartitions(partitions));
                case "Unknown" -> {}
                default ->
                    consumers.put(group.groupId(), new ListConsumerGroupOffsetsSpec().topicPartitions(partitions));
            }
        }
        if (!consumers.isEmpty()) {
            context.await()
                    .get(context.admin()
                            .listConsumerGroupOffsets(
                                    consumers, new ListConsumerGroupOffsetsOptions().timeoutMs(timeout))
                            .all())
                    .forEach((group, offsets) -> add(lost, group, start, target, offsets));
        }
        if (!streams.isEmpty()) {
            context.await()
                    .get(context.admin()
                            .listStreamsGroupOffsets(streams, new ListStreamsGroupOffsetsOptions().timeoutMs(timeout))
                            .all())
                    .forEach((group, offsets) -> add(lost, group, start, target, offsets));
        }
        if (!shares.isEmpty()) {
            Map<String, Map<TopicPartition, OffsetAndMetadata>> converted = new HashMap<>();
            context.await()
                    .get(context.admin()
                            .listShareGroupOffsets(shares, new ListShareGroupOffsetsOptions().timeoutMs(timeout))
                            .all())
                    .forEach((group, offsets) -> {
                        Map<TopicPartition, OffsetAndMetadata> at = new HashMap<>();
                        offsets.forEach((tp, info) -> {
                            if (info != null) {
                                at.put(tp, new OffsetAndMetadata(info.startOffset()));
                            }
                        });
                        converted.put(group, at);
                    });
            converted.forEach((group, offsets) -> add(lost, group, start, target, offsets));
        }
        return lost;
    }

    private static void add(
            Map<String, Long> lost,
            String group,
            Map<TopicPartition, Long> start,
            Map<TopicPartition, Long> target,
            Map<TopicPartition, OffsetAndMetadata> offsets) {
        long total = 0;
        for (Map.Entry<TopicPartition, Long> e : target.entrySet()) {
            OffsetAndMetadata at = offsets.get(e.getKey());
            if (at != null) {
                long from = Math.max(at.offset(), start.get(e.getKey()));
                total += Math.max(0, e.getValue() - from);
            }
        }
        if (total > 0) {
            lost.put(group, total);
        }
    }

    static Preview preview(
            Context context, ResourceId id, String topic, Optional<Integer> only, Scope scope, Position position) {
        Plan plan = plan(context, topic, only, scope, position);
        List<AttributeChange> diff = new ArrayList<>();
        plan.target()
                .forEach((tp, offset) -> diff.add(new AttributeChange(
                        KafkaKinds.LOG_START_OFFSET + "." + tp.partition(),
                        Optional.of(plan.start().get(tp)),
                        Optional.of(offset))));
        plan.unreadLost()
                .forEach((group, count) ->
                        diff.add(new AttributeChange(UNREAD_LOST + group, Optional.of(0L), Optional.of(count))));
        return new Preview(
                diff,
                Set.of(),
                new Estimate(Count.estimate(plan.removed()), "{message}"),
                etag(context, id, topic, only, scope),
                PreconditionEnforcement.CHECKED_BEFORE);
    }

    /** The log start of every partition the truncation could touch: a truncation that ran since changes it. */
    static String etag(Context context, ResourceId id, String topic, Optional<Integer> only, Scope scope) {
        List<TopicPartition> named = Offsets.partitions(context, topic).stream()
                .filter(tp -> only.map(p -> p == tp.partition()).orElse(true))
                .toList();
        Map<TopicPartition, Long> start = Offsets.earliest(context, Seeks.partitionsIn(scope, named));
        Map<Integer, Long> byPartition = new LinkedHashMap<>();
        start.forEach((tp, offset) -> byPartition.put(tp.partition(), offset));
        return Etags.of(id.format(), byPartition);
    }

    static OperationResult execute(
            Context context, String topic, Optional<Integer> only, Scope scope, Position position) {
        Plan plan = plan(context, topic, only, scope, position);
        if (plan.target().isEmpty()) {
            return OperationResult.already();
        }
        Map<TopicPartition, RecordsToDelete> request = new HashMap<>();
        plan.target().forEach((tp, offset) -> request.put(tp, RecordsToDelete.beforeOffset(offset)));
        context.await()
                .get(context.admin()
                        .deleteRecords(
                                request,
                                new DeleteRecordsOptions()
                                        .timeoutMs(context.await().timeoutMs()))
                        .all());
        return OperationResult.affected(plan.removed());
    }
}
