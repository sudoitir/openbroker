package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.messaging.Position;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Condition;
import io.github.sudoitir.openbroker.model.ConditionStatus;
import io.github.sudoitir.openbroker.model.Kind;
import io.github.sudoitir.openbroker.model.MetricKeys;
import io.github.sudoitir.openbroker.model.MetricValue;
import io.github.sudoitir.openbroker.model.PartitionId;
import io.github.sudoitir.openbroker.model.Placement;
import io.github.sudoitir.openbroker.model.Precision;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.Shard;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import io.github.sudoitir.openbroker.operation.AttributeChange;
import io.github.sudoitir.openbroker.operation.Estimate;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import io.github.sudoitir.openbroker.operation.PreconditionEnforcement;
import io.github.sudoitir.openbroker.operation.Preview;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.TopicPartition;

/**
 * A group on a topic: the subscription. A subscription's backlog is its lag, as an estimate: the end
 * offset of each partition less where the group is, summed. The two kinds of group differ in where
 * "where the group is" is kept and how it is moved, so a subclass supplies those two.
 */
abstract class Subscriptions implements Handler {

    /** Why a lag is only an estimate. */
    static final String ESTIMATE_REASON =
            "The end offset moves while it is read, and transaction markers and compaction leave offsets that"
                    + " hold no record, so the lag can overcount.";

    /** Where a group stands on a partition, and the lag the cluster reports for it, if it does. */
    record Standing(long offset, OptionalLong lag) {}

    private final Kind kind;
    private final GroupType groupType;

    Subscriptions(Kind kind, GroupType groupType) {
        this.kind = kind;
        this.groupType = groupType;
    }

    @Override
    public Kind kind() {
        return kind;
    }

    @Override
    public Set<String> selectors() {
        return Set.of("group", "topic");
    }

    /** Where each group stands on each partition it has a place on. */
    abstract Map<String, Map<TopicPartition, Standing>> positions(Context context, Collection<String> groups);

    /** Moves where the group stands. Fails when the cluster refuses, for instance because the group has members. */
    abstract void move(Context context, String group, Map<TopicPartition, Long> offsets);

    /** Forgets where the group stands on the partitions. */
    abstract void forget(Context context, String group, Set<TopicPartition> partitions);

    /** The word for the offsets this kind keeps. */
    abstract String offsetsName();

    ResourceId id(String group, String topic) {
        return new ResourceId(kind.id(), ResourcePath.of(group, topic));
    }

    private boolean accepts(GroupListing listing) {
        GroupType type = GroupInfo.typeOf(listing);
        return groupType.equals(type) || (GroupType.CONSUMER.equals(groupType) && GroupType.CLASSIC.equals(type));
    }

    private Map<String, GroupInfo> groups(Context context, Optional<String> only) {
        List<GroupListing> listings = GroupInfo.list(context).stream()
                .filter(this::accepts)
                .filter(g -> only.map(g.groupId()::equals).orElse(true))
                .toList();
        Map<String, GroupInfo> described = GroupInfo.describe(context, listings);
        // A classic group run by Kafka Streams is read-only: it is no subscription.
        described.values().removeIf(g -> !g.manageable());
        return described;
    }

    @Override
    public List<Candidate> candidates(Context context, Listing listing) {
        Map<String, GroupInfo> groups = groups(context, listing.field("group"));
        Map<String, Map<TopicPartition, Standing>> positions = positions(context, groups.keySet());
        var match = listing.names();
        Optional<String> topicOnly = listing.field("topic");
        List<Candidate> out = new ArrayList<>();
        for (GroupInfo group : groups.values()) {
            Set<String> topics = new LinkedHashSet<>(group.topics());
            positions.getOrDefault(group.id(), Map.of()).keySet().forEach(tp -> topics.add(tp.topic()));
            for (String topic : topics) {
                if (match.test(topic) && topicOnly.map(topic::equals).orElse(true)) {
                    out.add(new Candidate(
                            id(group.id(), topic),
                            List.of(Placement.instance(KafkaCluster.CLUSTER, Attributes.empty())),
                            Attributes.empty()));
                }
            }
        }
        out.sort(Comparator.comparing(c -> c.id().format()));
        return out;
    }

    @Override
    public List<Resource> resources(Context context, List<Candidate> page, Listing listing) {
        if (page.isEmpty()) {
            return List.of();
        }
        Set<String> groupIds = new HashSet<>();
        Set<String> topics = new HashSet<>();
        for (Candidate c : page) {
            groupIds.add(c.id().path().segments().get(0));
            topics.add(c.id().name());
        }
        List<GroupListing> listings = GroupInfo.list(context).stream()
                .filter(g -> groupIds.contains(g.groupId()))
                .toList();
        Map<String, GroupInfo> groups = GroupInfo.describe(context, listings);
        Map<String, Map<TopicPartition, Standing>> positions = positions(context, groupIds);
        Map<String, List<TopicPartition>> partitions = new HashMap<>();
        List<TopicPartition> all = new ArrayList<>();
        for (String topic : topics) {
            try {
                List<TopicPartition> ps = Offsets.partitions(context, topic);
                partitions.put(topic, ps);
                all.addAll(ps);
            } catch (BrokerException e) {
                if (e.status() != Status.NOT_FOUND) {
                    throw e;
                }
                partitions.put(topic, List.of());
            }
        }
        Map<TopicPartition, Long> start = Offsets.earliest(context, all);
        Map<TopicPartition, Long> end = Offsets.latest(context, all);
        List<Resource> out = new ArrayList<>();
        for (Candidate candidate : page) {
            String group = candidate.id().path().segments().get(0);
            String topic = candidate.id().name();
            GroupInfo info = groups.get(group);
            if (info == null) {
                continue;
            }
            out.add(resource(
                    context,
                    candidate,
                    info,
                    topic,
                    partitions.get(topic),
                    positions.getOrDefault(group, Map.of()),
                    start,
                    end,
                    listing));
        }
        return out;
    }

    private Resource resource(
            Context context,
            Candidate candidate,
            GroupInfo info,
            String topic,
            List<TopicPartition> partitions,
            Map<TopicPartition, Standing> positions,
            Map<TopicPartition, Long> start,
            Map<TopicPartition, Long> end,
            Listing listing) {
        Map<Shard, Double> breakdown = new LinkedHashMap<>();
        Map<Integer, String> unavailable = new TreeMap<>();
        Map<Integer, Long> offsets = new TreeMap<>();
        double total = 0;
        for (TopicPartition tp : partitions) {
            Standing at = positions.get(tp);
            Long last = end.get(tp);
            Long first = start.get(tp);
            if (at == null) {
                unavailable.put(tp.partition(), "The group has no " + offsetsName() + " offset on this partition.");
            } else if (last == null || first == null) {
                unavailable.put(tp.partition(), "The partition's end offset could not be read.");
            } else {
                offsets.put(tp.partition(), at.offset());
                long lag = at.lag().orElse(Math.max(0, last - Math.max(at.offset(), first)));
                breakdown.put(Shard.partition(PartitionId.of(String.valueOf(tp.partition()))), (double) lag);
                total += lag;
            }
        }
        Attributes.Builder attributes = Attributes.builder()
                .put(MetricKeys.BACKLOG, new MetricValue(total, Precision.ESTIMATE, Optional.empty(), breakdown))
                .put(MetricKeys.CONSUMER_COUNT, MetricValue.exact(info.members().size()))
                .put(StandardAttributes.DESTINATION, Topics.id(topic))
                .put(StandardAttributes.PARENT, GroupInfo.resourceId(info.id()))
                .put(StandardAttributes.DURABLE, true)
                .putRaw(KafkaKinds.GROUP_STATE, info.state().toString().toLowerCase(Locale.ROOT))
                .putRaw(KafkaKinds.OFFSETS, Map.copyOf(offsets));
        info.epoch().ifPresent(e -> attributes.putRaw(KafkaKinds.GROUP_EPOCH, (long) e));
        if (!unavailable.isEmpty()) {
            attributes.putRaw(KafkaKinds.LAG_UNAVAILABLE, Map.copyOf(unavailable));
        }
        List<Condition> conditions = List.of(new Condition(
                KafkaKinds.BACKLOG_ESTIMATE,
                ConditionStatus.TRUE,
                "ESTIMATE",
                ESTIMATE_REASON
                        + (partitions.isEmpty()
                                ? " The topic does not exist, so there is no log to measure against."
                                : "")
                        + (unavailable.isEmpty()
                                ? ""
                                : " " + unavailable.size()
                                        + " partitions are left out of the total because their lag is unknown."),
                Optional.empty()));
        return new Resource(
                candidate.id(),
                Attrs.project(attributes.build(), listing.query().projection()),
                conditions,
                candidate.placements(),
                context.now());
    }

    @Override
    public Optional<Resource> find(Context context, ResourceId id) {
        Listing exact = new Listing(ResourceQuery.of(kind.id())
                .where(Map.of("group", id.path().segments().get(0), "topic", id.name())));
        List<Candidate> found = candidates(context, exact).stream()
                .filter(c -> c.id().equals(id))
                .toList();
        return resources(context, found, exact).stream().findFirst();
    }

    // Operations.

    /** The state a purge or seek acts on: the group's generation and members, and where it stands on the topic. */
    private record State(
            GroupInfo group, String topic, List<TopicPartition> partitions, Map<TopicPartition, Standing> positions) {}

    private State state(Context context, ResourceId id) {
        String group = id.path().segments().get(0);
        String topic = id.name();
        Map<String, GroupInfo> groups = groups(context, Optional.of(group));
        GroupInfo info = groups.get(group);
        if (info == null) {
            throw Handler.gone(id);
        }
        List<TopicPartition> partitions = Offsets.partitions(context, topic);
        Map<TopicPartition, Standing> at = new HashMap<>();
        positions(context, List.of(group)).getOrDefault(group, Map.of()).forEach((tp, p) -> {
            if (tp.topic().equals(topic)) {
                at.put(tp, p);
            }
        });
        return new State(info, topic, partitions, at);
    }

    @Override
    public String etag(Context context, ResourceId id, Operation operation, Scope scope) {
        State s = state(context, id);
        Map<Integer, Long> offsets = new TreeMap<>();
        s.positions().forEach((tp, p) -> offsets.put(tp.partition(), p.offset()));
        return Etags.of(
                id.format(),
                s.group().epoch(),
                s.group().state(),
                offsets,
                s.group().members().size());
    }

    @Override
    public Preview preview(Context context, ResourceId id, Operation operation, Scope scope) {
        switch (operation) {
            case Operation.Purge purge -> {
                return movePreview(context, id, scope, Position.latest());
            }
            case Operation.Seek seek -> {
                return movePreview(context, id, scope, seek.position());
            }
            case Operation.Create create -> {
                return new Preview(
                        Attrs.diff(Attributes.empty(), create.attributes()),
                        Set.of(),
                        new Estimate(Count.exact(1), "{subscription}"),
                        Etags.of(id.format(), "absent"),
                        PreconditionEnforcement.CHECKED_BEFORE);
            }
            case Operation.Delete delete -> {
                State s = state(context, id);
                List<AttributeChange> diff = new ArrayList<>();
                diff.add(new AttributeChange(KafkaKinds.OFFSETS, Optional.of(offsetsOf(s)), Optional.empty()));
                members(s).ifPresent(diff::add);
                return new Preview(
                        diff,
                        Set.of(),
                        new Estimate(Count.exact(1), "{subscription}"),
                        etag(context, id, operation, scope),
                        PreconditionEnforcement.CHECKED_BEFORE);
            }
            default -> throw unsupported(operation);
        }
    }

    private static Map<Integer, Long> offsetsOf(State s) {
        Map<Integer, Long> offsets = new TreeMap<>();
        s.positions().forEach((tp, p) -> offsets.put(tp.partition(), p.offset()));
        return offsets;
    }

    /** An empty classic group names no assignor, so the preview says what a Kafka Streams application needs instead. */
    private static Optional<AttributeChange> streamsNote(State s) {
        return GroupType.CLASSIC.equals(s.group().type()) && s.group().members().isEmpty()
                ? Optional.of(new AttributeChange(
                        KafkaKinds.NOTE,
                        Optional.empty(),
                        Optional.of("If this group belongs to a Kafka Streams application, reset that application's"
                                + " state with Kafka's application reset tool instead: moving offsets alone leaves its"
                                + " state stores behind.")))
                : Optional.empty();
    }

    private static Optional<AttributeChange> members(State s) {
        return s.group().members().isEmpty()
                ? Optional.empty()
                : Optional.of(new AttributeChange(
                        KafkaKinds.MEMBERS,
                        Optional.of(s.group().members().stream()
                                .map(GroupInfo.Member::describe)
                                .toList()),
                        Optional.empty()));
    }

    private Preview movePreview(Context context, ResourceId id, Scope scope, Position position) {
        State s = state(context, id);
        Map<TopicPartition, Long> target = Seeks.resolve(context, Seeks.partitionsIn(scope, s.partitions()), position);
        List<AttributeChange> diff = new ArrayList<>();
        long records = 0;
        for (Map.Entry<TopicPartition, Long> e : target.entrySet()) {
            Standing before = s.positions().get(e.getKey());
            diff.add(new AttributeChange(
                    KafkaKinds.OFFSETS + "." + e.getKey().partition(),
                    before == null ? Optional.empty() : Optional.of(before.offset()),
                    Optional.of(e.getValue())));
            records += before == null ? 0 : Math.abs(e.getValue() - before.offset());
        }
        members(s).ifPresent(diff::add);
        streamsNote(s).ifPresent(diff::add);
        return new Preview(
                diff,
                Set.of(),
                new Estimate(Count.estimate(records), "{message}"),
                etag(context, id, new Operation.Purge(), scope),
                PreconditionEnforcement.CHECKED_BEFORE);
    }

    @Override
    public OperationResult execute(Context context, ResourceId id, Operation operation, Scope scope) {
        return switch (operation) {
            case Operation.Purge purge -> shift(context, id, scope, Position.latest());
            case Operation.Seek seek -> shift(context, id, scope, seek.position());
            case Operation.Delete delete -> delete(context, id, delete);
            case Operation.Create create -> create(context, id, create.attributes());
            default -> throw unsupported(operation);
        };
    }

    /** Refuses to act on a group that has members or is rebalancing: Kafka moves offsets only for an idle one. */
    static void requireIdle(ResourceId id, GroupInfo group, String what) {
        if (!group.members().isEmpty()) {
            throw KafkaErrors.refused(
                    Status.FAILED_PRECONDITION,
                    ErrorReasons.NOT_EMPTY,
                    "Cannot " + what + " " + id + " while " + group.id() + " has active members: "
                            + group.members().stream()
                                    .map(GroupInfo.Member::describe)
                                    .toList()
                            + ". Stop them first.");
        }
        if (group.rebalancing()) {
            throw KafkaErrors.refused(
                    Status.FAILED_PRECONDITION,
                    ErrorReasons.NOT_EMPTY,
                    "Cannot " + what + " " + id + " while " + group.id() + " is rebalancing (" + group.state()
                            + "). Try again once it settles.");
        }
    }

    private OperationResult shift(Context context, ResourceId id, Scope scope, Position position) {
        State s = state(context, id);
        requireIdle(id, s.group(), "move the offsets of");
        Map<TopicPartition, Long> target = Seeks.resolve(context, Seeks.partitionsIn(scope, s.partitions()), position);
        if (target.isEmpty()) {
            return OperationResult.already();
        }
        move(context, s.group().id(), target);
        return OperationResult.affected(target.size());
    }

    private OperationResult delete(Context context, ResourceId id, Operation.Delete delete) {
        State s = state(context, id);
        if (delete.ifEmpty()) {
            Resource resource = find(context, id).orElseThrow(() -> Handler.gone(id));
            MetricValue backlog = resource.attributes().get(MetricKeys.BACKLOG).orElseThrow();
            if (backlog.asLong() > 0) {
                throw KafkaErrors.refused(
                        Status.FAILED_PRECONDITION,
                        ErrorReasons.NOT_EMPTY,
                        id + " still has about " + backlog.asLong() + " messages to read.");
            }
        }
        if (!s.group().members().isEmpty()) {
            if (!delete.disconnectConsumers()
                    || GroupType.SHARE.equals(s.group().type())) {
                requireIdle(id, s.group(), "delete");
            }
            Groups.removeMembers(context, s.group().id());
        }
        forget(context, s.group().id(), s.positions().keySet());
        return OperationResult.done();
    }

    /** Makes the subscription; a kind that cannot be made by hand refuses. */
    OperationResult create(Context context, ResourceId id, Attributes attributes) {
        throw unsupported(new Operation.Create(attributes));
    }
}
