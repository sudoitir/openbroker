package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.model.Kind;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.kafka.clients.admin.AlterShareGroupOffsetsOptions;
import org.apache.kafka.clients.admin.DeleteShareGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.SharePartitionOffsetInfo;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.TopicPartition;

/** A share group on a topic: the group's start offsets, with the lag the cluster reports. */
final class ShareSubscriptions extends Subscriptions {

    ShareSubscriptions(Kind kind) {
        super(kind, GroupType.SHARE);
    }

    @Override
    String offsetsName() {
        return "share";
    }

    @Override
    Map<String, Map<TopicPartition, Standing>> ask(Context context, Collection<String> groups) {
        if (groups.isEmpty()) {
            return Map.of();
        }
        Map<String, ListShareGroupOffsetsSpec> specs = new HashMap<>();
        groups.forEach(g -> specs.put(g, new ListShareGroupOffsetsSpec()));
        Map<String, Map<TopicPartition, SharePartitionOffsetInfo>> found = context.await()
                .get(context.admin()
                        .listShareGroupOffsets(
                                specs,
                                new ListShareGroupOffsetsOptions()
                                        .timeoutMs(context.await().timeoutMs()))
                        .all());
        Map<String, Map<TopicPartition, Standing>> out = new HashMap<>();
        found.forEach((group, offsets) -> {
            Map<TopicPartition, Standing> at = new HashMap<>();
            offsets.forEach((tp, info) -> {
                // A partition no member has fetched from yet has no share state, so no start offset.
                if (info != null) {
                    at.put(
                            tp,
                            new Standing(
                                    info.startOffset(),
                                    info.lag().map(OptionalLong::of).orElse(OptionalLong.empty())));
                }
            });
            out.put(group, at);
        });
        return out;
    }

    @Override
    void move(Context context, String group, Map<TopicPartition, Long> offsets) {
        context.await()
                .get(context.admin()
                        .alterShareGroupOffsets(
                                group,
                                offsets,
                                new AlterShareGroupOffsetsOptions()
                                        .timeoutMs(context.await().timeoutMs()))
                        .all());
    }

    @Override
    void forget(Context context, String group, Set<TopicPartition> partitions) {
        Set<String> topics = new HashSet<>();
        partitions.forEach(tp -> topics.add(tp.topic()));
        if (topics.isEmpty()) {
            return;
        }
        context.await()
                .get(context.admin()
                        .deleteShareGroupOffsets(
                                group,
                                topics,
                                new DeleteShareGroupOffsetsOptions()
                                        .timeoutMs(context.await().timeoutMs()))
                        .all());
    }
}
