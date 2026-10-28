package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeShareGroupsOptions;
import org.apache.kafka.clients.admin.DescribeStreamsGroupsOptions;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.ShareMemberDescription;
import org.apache.kafka.clients.admin.StreamsGroupDescription;
import org.apache.kafka.clients.admin.StreamsGroupMemberDescription;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.Node;

/** What the cluster says about one group: its type, state, generation and members. */
record GroupInfo(
        String id,
        GroupType type,
        GroupState state,
        OptionalInt epoch,
        List<Member> members,
        OptionalInt coordinator,
        String assignor,
        Set<String> topics) {

    static final String OWNED_BY_APPLICATION =
            "Its offsets belong to the application that runs it, so this driver reads them and does not change them.";

    static final String OWNED_BY_STREAMS_APPLICATION =
            "It is run by a Kafka Streams application, which owns its offsets and state, so this driver reads them and"
                    + " does not change them.";

    /** The assignor a Kafka Streams application's classic group names. */
    private static final String STREAMS_ASSIGNOR = "stream";

    /** A running member of a group, as an operator names it. */
    record Member(String id, String clientId, String host, Set<String> topics) {

        Member {
            topics = Set.copyOf(topics);
        }

        String describe() {
            return clientId + "@" + host + " (" + id + ")";
        }
    }

    GroupInfo {
        members = List.copyOf(members);
        topics = Set.copyOf(topics);
    }

    static ResourceId resourceId(String group) {
        return new ResourceId(KafkaKinds.GROUP, ResourcePath.of(group));
    }

    /**
     * Whether the driver manages this group's offsets: consumer, classic and share groups. A streams group, and
     * a classic group whose assignor is Kafka Streams', belong to their application.
     */
    boolean manageable() {
        return (GroupType.CONSUMER.equals(type) || GroupType.SHARE.equals(type) || GroupType.CLASSIC.equals(type))
                && !STREAMS_ASSIGNOR.equals(assignor);
    }

    /** Why the driver does not change this group's offsets; empty when it does. */
    String readOnlyReason() {
        if (manageable()) {
            return "";
        }
        return GroupType.STREAMS.equals(type) ? OWNED_BY_APPLICATION : OWNED_BY_STREAMS_APPLICATION;
    }

    /** A group's type; a cluster before Kafka 3.7 does not list one, and its groups are classic. */
    static GroupType typeOf(GroupListing listing) {
        return listing.type().orElse(GroupType.CLASSIC);
    }

    /** Whether the group is rebalancing: its membership is about to change. */
    boolean rebalancing() {
        return switch (state) {
            case PREPARING_REBALANCE, COMPLETING_REBALANCE, ASSIGNING, RECONCILING -> true;
            default -> false;
        };
    }

    static List<GroupListing> list(Context context) {
        Reads reads = context.reads();
        return reads != null ? reads.listings(context) : ask(context);
    }

    /** Asks the cluster for the listing of every group. */
    static List<GroupListing> ask(Context context) {
        return List.copyOf(context.await()
                .get(context.admin()
                        .listGroups(new ListGroupsOptions()
                                .timeoutMs(context.await().timeoutMs()))
                        .all()));
    }

    /** Describes the groups, each by the way its type is described. */
    static Map<String, GroupInfo> describe(Context context, Collection<GroupListing> listings) {
        Reads reads = context.reads();
        return reads != null ? reads.describe(context, listings) : ask(context, listings);
    }

    /** Asks the cluster to describe the groups, in one round for each type. */
    static Map<String, GroupInfo> ask(Context context, Collection<GroupListing> listings) {
        List<String> consumers = new ArrayList<>();
        List<String> shares = new ArrayList<>();
        List<String> streams = new ArrayList<>();
        Map<String, GroupListing> byId = new HashMap<>();
        for (GroupListing listing : listings) {
            byId.put(listing.groupId(), listing);
            switch (typeOf(listing).toString()) {
                case "Share" -> shares.add(listing.groupId());
                case "Streams" -> streams.add(listing.groupId());
                case "Unknown" -> {}
                default -> consumers.add(listing.groupId());
            }
        }
        Map<String, GroupInfo> out = new HashMap<>();
        int timeout = context.await().timeoutMs();
        if (!consumers.isEmpty()) {
            context.await()
                    .get(context.admin()
                            .describeConsumerGroups(consumers, new DescribeConsumerGroupsOptions().timeoutMs(timeout))
                            .all())
                    .forEach((id, d) -> out.put(id, of(d)));
        }
        if (!shares.isEmpty()) {
            context.await()
                    .get(context.admin()
                            .describeShareGroups(shares, new DescribeShareGroupsOptions().timeoutMs(timeout))
                            .all())
                    .forEach((id, d) -> out.put(id, of(d)));
        }
        if (!streams.isEmpty()) {
            context.await()
                    .get(context.admin()
                            .describeStreamsGroups(streams, new DescribeStreamsGroupsOptions().timeoutMs(timeout))
                            .all())
                    .forEach((id, d) -> out.put(id, of(d)));
        }
        return out;
    }

    private static GroupInfo of(ConsumerGroupDescription d) {
        List<Member> members = new ArrayList<>();
        Set<String> topics = new HashSet<>();
        for (MemberDescription m : d.members()) {
            Set<String> own = new HashSet<>();
            m.assignment().topicPartitions().forEach(tp -> own.add(tp.topic()));
            members.add(new Member(m.consumerId(), m.clientId(), m.host(), own));
            topics.addAll(own);
        }
        return new GroupInfo(
                d.groupId(),
                d.type(),
                d.groupState(),
                d.groupEpoch().map(OptionalInt::of).orElse(OptionalInt.empty()),
                members,
                coordinator(d.coordinator()),
                d.partitionAssignor(),
                topics);
    }

    private static GroupInfo of(ShareGroupDescription d) {
        List<Member> members = new ArrayList<>();
        Set<String> topics = new HashSet<>();
        for (ShareMemberDescription m : d.members()) {
            Set<String> own = new HashSet<>();
            m.assignment().topicPartitions().forEach(tp -> own.add(tp.topic()));
            members.add(new Member(m.consumerId(), m.clientId(), m.host(), own));
            topics.addAll(own);
        }
        return new GroupInfo(
                d.groupId(),
                GroupType.SHARE,
                d.groupState(),
                OptionalInt.of(d.groupEpoch()),
                members,
                coordinator(d.coordinator()),
                "",
                topics);
    }

    private static GroupInfo of(StreamsGroupDescription d) {
        List<Member> members = new ArrayList<>();
        for (StreamsGroupMemberDescription m : d.members()) {
            members.add(new Member(m.memberId(), m.clientId(), "", Set.of()));
        }
        return new GroupInfo(
                d.groupId(),
                GroupType.STREAMS,
                d.groupState(),
                OptionalInt.of(d.groupEpoch()),
                members,
                coordinator(d.coordinator()),
                d.assignorName().orElse(""),
                Set.of());
    }

    private static OptionalInt coordinator(Node node) {
        return node == null || node.isEmpty() ? OptionalInt.empty() : OptionalInt.of(node.id());
    }
}
