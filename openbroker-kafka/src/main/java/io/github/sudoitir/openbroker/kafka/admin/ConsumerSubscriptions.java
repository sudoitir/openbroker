package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Kind;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.operation.OperationResult;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupSubscribedToTopicException;
import org.apache.kafka.common.errors.UnknownMemberIdException;

/**
 * A consumer group on a topic: the group's committed offsets. The group is a consumer-protocol
 * group or a classic group, whether made by committing offsets alone or run by an application: Kafka
 * moves its offsets only while it is empty and settled. A classic group run by Kafka Streams is left
 * out.
 */
final class ConsumerSubscriptions extends Subscriptions {

    ConsumerSubscriptions(Kind kind) {
        super(kind, GroupType.CONSUMER);
    }

    @Override
    String offsetsName() {
        return "committed";
    }

    @Override
    Map<String, Map<TopicPartition, Standing>> positions(Context context, Collection<String> groups) {
        if (groups.isEmpty()) {
            return Map.of();
        }
        Map<String, ListConsumerGroupOffsetsSpec> specs = new HashMap<>();
        groups.forEach(g -> specs.put(g, new ListConsumerGroupOffsetsSpec()));
        Map<String, Map<TopicPartition, OffsetAndMetadata>> found = context.await()
                .get(context.admin()
                        .listConsumerGroupOffsets(
                                specs,
                                new ListConsumerGroupOffsetsOptions()
                                        .timeoutMs(context.await().timeoutMs()))
                        .all());
        Map<String, Map<TopicPartition, Standing>> out = new HashMap<>();
        found.forEach((group, offsets) -> {
            Map<TopicPartition, Standing> at = new HashMap<>();
            offsets.forEach((tp, o) -> {
                if (o != null) {
                    at.put(tp, new Standing(o.offset(), OptionalLong.empty()));
                }
            });
            out.put(group, at);
        });
        return out;
    }

    @Override
    void move(Context context, String group, Map<TopicPartition, Long> offsets) {
        Map<TopicPartition, OffsetAndMetadata> target = new HashMap<>();
        offsets.forEach((tp, offset) -> target.put(tp, new OffsetAndMetadata(offset)));
        try {
            context.await()
                    .get(context.admin()
                            .alterConsumerGroupOffsets(
                                    group,
                                    target,
                                    new AlterConsumerGroupOffsetsOptions()
                                            .timeoutMs(context.await().timeoutMs()))
                            .all());
        } catch (BrokerException e) {
            throw busy(e, group);
        }
    }

    @Override
    void forget(Context context, String group, Set<TopicPartition> partitions) {
        if (partitions.isEmpty()) {
            return;
        }
        try {
            context.await()
                    .get(context.admin()
                            .deleteConsumerGroupOffsets(
                                    group,
                                    partitions,
                                    new DeleteConsumerGroupOffsetsOptions()
                                            .timeoutMs(context.await().timeoutMs()))
                            .all());
        } catch (BrokerException e) {
            throw busy(e, group);
        }
    }

    /** A cluster that refuses because the group is in use says so in its own words; this is the SDK's. */
    private static BrokerException busy(BrokerException e, String group) {
        if (e.getCause() instanceof UnknownMemberIdException
                || e.getCause() instanceof GroupSubscribedToTopicException) {
            return KafkaErrors.refused(
                    Status.FAILED_PRECONDITION,
                    ErrorReasons.NOT_EMPTY,
                    "The cluster refuses to change group " + group + " while it has active members.");
        }
        return e;
    }

    @Override
    OperationResult create(Context context, ResourceId id, Attributes attributes) {
        String group = id.path().segments().get(0);
        String topic = id.name();
        Optional<GroupInfo> existing = existing(context, group);
        if (existing.isPresent() && !existing.get().manageable()) {
            throw KafkaErrors.refused(
                    Status.FAILED_PRECONDITION,
                    ErrorReasons.PRECONDITION_FAILED,
                    group + " is a " + Groups.typeName(existing.get().type()) + " group. "
                            + existing.get().readOnlyReason());
        }
        List<TopicPartition> partitions;
        try {
            partitions = Offsets.partitions(context, topic);
        } catch (BrokerException e) {
            if (e.status() != Status.NOT_FOUND) {
                throw e;
            }
            Topics.create(context, Topics.id(topic), attributes);
            partitions = Offsets.partitions(context, topic);
        }
        Map<TopicPartition, Standing> have = new HashMap<>();
        positions(context, List.of(group)).getOrDefault(group, Map.of()).forEach((tp, p) -> {
            if (tp.topic().equals(topic)) {
                have.put(tp, p);
            }
        });
        List<TopicPartition> missing =
                partitions.stream().filter(tp -> !have.containsKey(tp)).toList();
        if (missing.isEmpty()) {
            return OperationResult.already();
        }
        if (existing.isPresent() && !existing.get().members().isEmpty()) {
            throw KafkaErrors.refused(
                    Status.FAILED_PRECONDITION,
                    ErrorReasons.NOT_EMPTY,
                    group + " has active members; it joins " + topic + " itself.");
        }
        move(context, group, Offsets.earliest(context, missing));
        return OperationResult.affected(missing.size());
    }

    private static Optional<GroupInfo> existing(Context context, String group) {
        try {
            return Optional.of(Groups.info(context, group));
        } catch (BrokerException e) {
            if (e.status() == Status.NOT_FOUND) {
                return Optional.empty();
            }
            throw e;
        }
    }
}
