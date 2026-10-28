package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Kind;
import io.github.sudoitir.openbroker.model.MetricKeys;
import io.github.sudoitir.openbroker.model.MetricValue;
import io.github.sudoitir.openbroker.model.Placement;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Estimate;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import io.github.sudoitir.openbroker.operation.PreconditionEnforcement;
import io.github.sudoitir.openbroker.operation.Preview;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DeleteShareGroupsOptions;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.common.GroupType;

/** Groups of every type: consumer, classic, share and streams. */
final class Groups implements Handler {

    private final Kind kind;

    Groups(Kind kind) {
        this.kind = kind;
    }

    @Override
    public Kind kind() {
        return kind;
    }

    @Override
    public Set<String> selectors() {
        return Set.of("type", "state");
    }

    static String typeName(GroupType type) {
        return type.toString().toLowerCase(Locale.ROOT);
    }

    @Override
    public List<Candidate> candidates(Context context, Listing listing) {
        var match = listing.names();
        Optional<String> type = listing.field("type");
        Optional<String> state = listing.field("state");
        List<Candidate> out = new ArrayList<>();
        for (GroupListing group : GroupInfo.list(context)) {
            String groupType = typeName(group.type().orElse(GroupType.UNKNOWN));
            String groupState = group.groupState()
                    .map(s -> s.toString().toLowerCase(Locale.ROOT))
                    .orElse("unknown");
            if (match.test(group.groupId())
                    && type.map(groupType::equalsIgnoreCase).orElse(true)
                    && state.map(groupState::equalsIgnoreCase).orElse(true)) {
                out.add(new Candidate(
                        GroupInfo.resourceId(group.groupId()),
                        List.of(Placement.instance(KafkaCluster.CLUSTER, Attributes.empty())),
                        Attributes.builder()
                                .putRaw(KafkaKinds.GROUP_TYPE, groupType)
                                .putRaw(KafkaKinds.GROUP_STATE, groupState)
                                .build()));
            }
        }
        out.sort(Comparator.comparing(c -> c.id().name()));
        return out;
    }

    @Override
    public List<Resource> resources(Context context, List<Candidate> page, Listing listing) {
        if (page.isEmpty()) {
            return List.of();
        }
        Set<String> names = new HashSet<>();
        page.forEach(c -> names.add(c.id().name()));
        List<GroupListing> listings = GroupInfo.list(context).stream()
                .filter(g -> names.contains(g.groupId()))
                .toList();
        Map<String, GroupInfo> described = GroupInfo.describe(context, listings);
        List<Resource> out = new ArrayList<>();
        for (Candidate candidate : page) {
            Attributes.Builder attributes = Attributes.builder().putAll(candidate.attributes());
            GroupInfo info = described.get(candidate.id().name());
            if (info != null) {
                attributes.putRaw(KafkaKinds.MEMBERS, (long) info.members().size());
                attributes.put(
                        MetricKeys.CONSUMER_COUNT,
                        MetricValue.exact(info.members().size()));
                info.epoch().ifPresent(e -> attributes.putRaw(KafkaKinds.GROUP_EPOCH, (long) e));
                info.coordinator().ifPresent(c -> attributes.putRaw(KafkaKinds.COORDINATOR, (long) c));
                if (!info.assignor().isEmpty()) {
                    attributes.putRaw(KafkaKinds.ASSIGNOR, info.assignor());
                }
                if (!info.manageable()) {
                    attributes.putRaw(KafkaKinds.READ_ONLY_REASON, info.readOnlyReason());
                }
            }
            out.add(new Resource(
                    candidate.id(),
                    Attrs.project(attributes.build(), listing.query().projection()),
                    List.of(),
                    candidate.placements(),
                    context.now()));
        }
        return out;
    }

    @Override
    public Optional<Resource> find(Context context, ResourceId id) {
        Listing exact = new Listing(ResourceQuery.of(KafkaKinds.GROUP));
        List<Candidate> found = candidates(context, exact).stream()
                .filter(c -> c.id().equals(id))
                .toList();
        return resources(context, found, exact).stream().findFirst();
    }

    /** The description of a group; its listing is needed to know how to ask. */
    static GroupInfo info(Context context, String group) {
        List<GroupListing> listings = GroupInfo.list(context).stream()
                .filter(g -> g.groupId().equals(group))
                .toList();
        if (listings.isEmpty()) {
            throw Handler.gone(GroupInfo.resourceId(group));
        }
        GroupInfo info = GroupInfo.describe(context, listings).get(group);
        if (info == null) {
            throw Handler.gone(GroupInfo.resourceId(group));
        }
        return info;
    }

    @Override
    public Preview preview(Context context, ResourceId id, Operation operation, Scope scope) {
        if (!(operation instanceof Operation.Delete)) {
            throw unsupported(operation);
        }
        GroupInfo info = manageable(context, id);
        Attributes.Builder before = Attributes.builder()
                .putRaw(KafkaKinds.GROUP_STATE, info.state().toString())
                .putRaw(
                        KafkaKinds.MEMBERS,
                        info.members().stream().map(GroupInfo.Member::describe).toList());
        return new Preview(
                Attrs.diff(before.build(), Attributes.empty()),
                Set.of(),
                new Estimate(Count.exact(1), "{group}"),
                etag(context, id, operation, scope),
                PreconditionEnforcement.CHECKED_BEFORE);
    }

    @Override
    public String etag(Context context, ResourceId id, Operation operation, Scope scope) {
        GroupInfo info = info(context, id.name());
        return Etags.of(id.format(), info.state(), info.epoch(), info.members());
    }

    @Override
    public OperationResult execute(Context context, ResourceId id, Operation operation, Scope scope) {
        if (!(operation instanceof Operation.Delete delete)) {
            throw refusePurgeOrSeek(context, id, operation);
        }
        GroupInfo info = manageable(context, id);
        if (!info.members().isEmpty()) {
            if (!delete.disconnectConsumers() || GroupType.SHARE.equals(info.type())) {
                throw KafkaErrors.refused(
                        Status.FAILED_PRECONDITION,
                        ErrorReasons.HAS_CONSUMERS,
                        id + " has active members: "
                                + info.members().stream()
                                        .map(GroupInfo.Member::describe)
                                        .toList() + ".");
            }
            removeMembers(context, id.name());
        }
        int timeout = context.await().timeoutMs();
        if (GroupType.SHARE.equals(info.type())) {
            context.await()
                    .get(context.admin()
                            .deleteShareGroups(List.of(id.name()), new DeleteShareGroupsOptions().timeoutMs(timeout))
                            .all());
        } else {
            context.await()
                    .get(context.admin()
                            .deleteConsumerGroups(
                                    List.of(id.name()), new DeleteConsumerGroupsOptions().timeoutMs(timeout))
                            .all());
        }
        return OperationResult.done();
    }

    /** The group, if the driver changes its offsets; a streams group is refused with why. */
    private GroupInfo manageable(Context context, ResourceId id) {
        GroupInfo info = info(context, id.name());
        if (!info.manageable()) {
            throw KafkaErrors.refused(
                    Status.UNIMPLEMENTED,
                    ErrorReasons.NOT_SUPPORTED,
                    id + " is a " + Groups.typeName(info.type()) + " group. " + info.readOnlyReason());
        }
        return info;
    }

    private BrokerException refusePurgeOrSeek(Context context, ResourceId id, Operation operation) {
        if (operation instanceof Operation.Purge || operation instanceof Operation.Seek) {
            GroupInfo info = info(context, id.name());
            return KafkaErrors.refused(
                    Status.UNIMPLEMENTED,
                    ErrorReasons.NOT_SUPPORTED,
                    info.manageable()
                            ? "Purge or seek a group's subscription to a topic, not the group: " + id
                                    + " reads several."
                            : id + " is a " + typeName(info.type()) + " group. " + info.readOnlyReason());
        }
        return unsupported(operation);
    }

    static void removeMembers(Context context, String group) {
        context.await()
                .get(context.admin()
                        .removeMembersFromConsumerGroup(
                                group,
                                new RemoveMembersFromConsumerGroupOptions()
                                        .timeoutMs(context.await().timeoutMs()))
                        .all());
    }
}
