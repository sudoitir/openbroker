package io.github.sudoitir.openbroker.kafka.admin;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;

/**
 * What one {@code read} of several queries has already asked the cluster, so that the queries share
 * their rounds: the group listing, the description and offsets of each group, the partitions of each
 * topic and their log offsets. It lives for that one call and is used from one thread, so a read
 * never sees older data than the call itself began with.
 */
final class Reads {

    private @Nullable List<GroupListing> listings;
    private final Map<String, GroupInfo> described = new HashMap<>();
    private final Map<String, Map<String, Map<TopicPartition, Subscriptions.Standing>>> positions = new HashMap<>();
    private final Map<String, List<TopicPartition>> partitions = new HashMap<>();
    private final Map<TopicPartition, Long> earliest = new HashMap<>();
    private final Map<TopicPartition, Long> latest = new HashMap<>();

    /** The listing of every group, asked once. */
    List<GroupListing> listings(Context context) {
        if (listings == null) {
            listings = GroupInfo.ask(context);
        }
        return listings;
    }

    /** The groups described, asking in one round only for those not described yet. */
    Map<String, GroupInfo> describe(Context context, Collection<GroupListing> wanted) {
        List<GroupListing> missing =
                wanted.stream().filter(l -> !described.containsKey(l.groupId())).toList();
        if (!missing.isEmpty()) {
            described.putAll(GroupInfo.ask(context, missing));
        }
        Map<String, GroupInfo> out = new HashMap<>();
        for (GroupListing listing : wanted) {
            GroupInfo info = described.get(listing.groupId());
            if (info != null) {
                out.put(listing.groupId(), info);
            }
        }
        return out;
    }

    /** Where the groups stand, asking in one round only for those not read yet by this kind of subscription. */
    Map<String, Map<TopicPartition, Subscriptions.Standing>> positions(
            Context context, Subscriptions kind, Collection<String> groups) {
        Map<String, Map<TopicPartition, Subscriptions.Standing>> known =
                positions.computeIfAbsent(kind.kind().id().value(), k -> new HashMap<>());
        List<String> missing =
                groups.stream().filter(g -> !known.containsKey(g)).toList();
        if (!missing.isEmpty()) {
            Map<String, Map<TopicPartition, Subscriptions.Standing>> asked = kind.ask(context, missing);
            missing.forEach(g -> known.put(g, asked.getOrDefault(g, Map.of())));
        }
        Map<String, Map<TopicPartition, Subscriptions.Standing>> out = new HashMap<>();
        groups.forEach(g -> out.put(g, known.get(g)));
        return out;
    }

    /** The partitions of the topics, one round for those not known yet; a topic that does not exist has none. */
    Map<String, List<TopicPartition>> partitions(Context context, Collection<String> topics) {
        List<String> missing =
                topics.stream().filter(t -> !partitions.containsKey(t)).toList();
        if (!missing.isEmpty()) {
            partitions.putAll(Offsets.ask(context, missing));
        }
        Map<String, List<TopicPartition>> out = new HashMap<>();
        topics.forEach(t -> out.put(t, partitions.get(t)));
        return out;
    }

    Map<TopicPartition, Long> earliest(Context context, Collection<TopicPartition> wanted) {
        return offsets(context, wanted, earliest, true);
    }

    Map<TopicPartition, Long> latest(Context context, Collection<TopicPartition> wanted) {
        return offsets(context, wanted, latest, false);
    }

    private static Map<TopicPartition, Long> offsets(
            Context context, Collection<TopicPartition> wanted, Map<TopicPartition, Long> known, boolean first) {
        List<TopicPartition> missing =
                wanted.stream().filter(p -> !known.containsKey(p)).toList();
        if (!missing.isEmpty()) {
            known.putAll(first ? Offsets.askEarliest(context, missing) : Offsets.askLatest(context, missing));
        }
        Map<TopicPartition, Long> out = new HashMap<>();
        for (TopicPartition p : wanted) {
            Long offset = known.get(p);
            if (offset != null) {
                out.put(p, offset);
            }
        }
        return out;
    }
}
