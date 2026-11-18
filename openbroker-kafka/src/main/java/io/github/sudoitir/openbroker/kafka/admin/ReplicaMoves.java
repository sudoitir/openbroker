package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.access.Capability;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.PartitionId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.operation.AttributeChange;
import io.github.sudoitir.openbroker.operation.Election;
import io.github.sudoitir.openbroker.operation.Estimate;
import io.github.sudoitir.openbroker.operation.EtagBasis;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import io.github.sudoitir.openbroker.operation.PreconditionEnforcement;
import io.github.sudoitir.openbroker.operation.Preview;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ElectLeadersOptions;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsOptions;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.jspecify.annotations.Nullable;

/**
 * Preferred leader election and a throttled replica move. Unclean election and an empty partition set are refused
 * before anything is sent. {@code electLeaders} is never called with a null set, which would elect every partition.
 * A move below {@code min.insync.replicas}, or without a positive throttle, is refused before any alter.
 */
final class ReplicaMoves {

    /** Operation option: bytes per second. Missing or not positive refuses the move. */
    static final String THROTTLE = "replication.throttle.bytes_per_second";

    static final String LEADER_RATE = "leader.replication.throttled.rate";
    static final String FOLLOWER_RATE = "follower.replication.throttled.rate";
    static final String LEADER_REPLICAS = "leader.replication.throttled.replicas";
    static final String FOLLOWER_REPLICAS = "follower.replication.throttled.replicas";

    private static final String MIN_IN_SYNC = "min.insync.replicas";

    private ReplicaMoves() {}

    /** One throttle key as it is now and as this move would set it. {@code previous} is empty when no override exists. */
    record ThrottleKey(ConfigResource resource, String key, Optional<String> previous, String desired) {

        ThrottleKey {
            previous = previous == null ? Optional.empty() : previous;
        }
    }

    /** A throttle key this call would write. {@code added} is true when there was no override to replace. */
    record ThrottleWrite(
            ConfigResource resource, String key, String applied, Optional<String> previous, boolean added) {}

    /**
     * The alter that puts back only the keys this call changed. An absent previous value deletes the override. A key
     * that is not in {@code changed} is not written.
     */
    static Map<ConfigResource, Collection<AlterConfigOp>> restore(List<ThrottleWrite> changed) {
        Map<ConfigResource, List<AlterConfigOp>> out = new LinkedHashMap<>();
        for (ThrottleWrite write : changed) {
            AlterConfigOp op = write.previous().isEmpty()
                    ? new AlterConfigOp(new ConfigEntry(write.key(), null), AlterConfigOp.OpType.DELETE)
                    : new AlterConfigOp(
                            new ConfigEntry(write.key(), write.previous().get()), AlterConfigOp.OpType.SET);
            out.computeIfAbsent(write.resource(), ignored -> new ArrayList<>()).add(op);
        }
        return new LinkedHashMap<>(out);
    }

    /** The write, or empty when the desired value is already the override or both are absent. */
    static Optional<ThrottleWrite> change(ThrottleKey key) {
        if (key.previous().isPresent() && key.previous().get().equals(key.desired())) {
            return Optional.empty();
        }
        if (key.previous().isEmpty() && key.desired().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new ThrottleWrite(
                key.resource(),
                key.key(),
                key.desired(),
                key.previous(),
                key.previous().isEmpty()));
    }

    static String identity(ConfigResource resource, String key) {
        if (resource.type() == ConfigResource.Type.BROKER) {
            return "broker." + resource.name() + "." + key;
        }
        return "topic." + key;
    }

    /** Previous overrides, and the keys this call would add. A pre-existing override is not marked added. */
    static Attributes throttleAttributes(List<ThrottleKey> keys) {
        Map<String, String> previous = new TreeMap<>();
        List<String> added = new ArrayList<>();
        for (ThrottleKey key : keys) {
            String name = identity(key.resource(), key.key());
            key.previous().ifPresent(value -> previous.put(name, value));
            change(key).filter(ThrottleWrite::added).ifPresent(write -> added.add(name));
        }
        return Attributes.builder()
                .putRaw(KafkaKinds.THROTTLE_PREVIOUS, Map.copyOf(previous))
                .putRaw(KafkaKinds.THROTTLE_ADDED, List.copyOf(added))
                .build();
    }

    /** Refuses unclean and an empty or null set. Neither reaches {@code electLeaders}. */
    static void checkElection(Election election, @Nullable Set<TopicPartition> partitions) {
        if (election != Election.PREFERRED) {
            throw refused("Kafka elects only the preferred replica. An unclean election can drop acknowledged records,"
                    + " so nothing is sent.");
        }
        if (partitions == null || partitions.isEmpty()) {
            throw refused("A preferred election names one partition. An empty set is refused, and electing every"
                    + " partition is not sent.");
        }
    }

    static boolean refused(Context context, Capability capability) {
        return context.cluster()
                .decided(capability)
                .filter(assessment -> assessment.status() == Assessment.Status.UNAVAILABLE)
                .isPresent();
    }

    /** A spec this connection already saw rejected is not sent again. */
    static void require(Context context, Capability capability) {
        context.cluster().decided(capability).ifPresent(assessment -> {
            if (assessment.status() == Assessment.Status.UNAVAILABLE) {
                throw KafkaErrors.refused(Status.UNIMPLEMENTED, ErrorReasons.UNSUPPORTED_VERSION, assessment.reason());
            }
        });
    }

    private static void noteUnsupported(Context context, Capability capability, BrokerException failure) {
        if (failure.reason().equals(ErrorReasons.UNSUPPORTED_VERSION)) {
            context.cluster().decide(capability, Assessment.unavailable(failure.getMessage(), Optional.empty()));
        }
    }

    /** In-progress reassignments for these partitions. An empty set is not a request for every partition. */
    static Map<TopicPartition, PartitionReassignment> reassignments(Context context, Set<TopicPartition> partitions) {
        if (partitions.isEmpty() || refused(context, KafkaAccess.REASSIGNMENT)) {
            return Map.of();
        }
        try {
            return context.await()
                    .get(context.admin()
                            .listPartitionReassignments(
                                    partitions,
                                    new ListPartitionReassignmentsOptions()
                                            .timeoutMs(context.await().timeoutMs()))
                            .reassignments());
        } catch (BrokerException e) {
            noteUnsupported(context, KafkaAccess.REASSIGNMENT, e);
            if (e.reason().equals(ErrorReasons.UNSUPPORTED_VERSION)) {
                return Map.of();
            }
            throw e;
        }
    }

    static String reassignmentId(@Nullable PartitionReassignment reassignment) {
        if (reassignment == null) {
            return "";
        }
        return "replicas=" + reassignment.replicas() + ",adding=" + reassignment.addingReplicas() + ",removing="
                + reassignment.removingReplicas();
    }

    static Preview previewElection(Context context, ResourceId id, Operation.ElectLeader election) {
        checkElection(election.election(), Set.of(Partitions.partition(id)));
        require(context, KafkaAccess.PREFERRED_ELECTION);
        TopicPartition tp = Partitions.partition(id);
        TopicPartitionInfo info = described(context, tp);
        refuseMoving(id, reassignments(context, Set.of(tp)).get(tp));
        return electionPreview(id, info);
    }

    static OperationResult elect(Context context, ResourceId id, Operation.ElectLeader election) {
        checkElection(election.election(), Set.of(Partitions.partition(id)));
        require(context, KafkaAccess.PREFERRED_ELECTION);
        TopicPartition tp = Partitions.partition(id);
        TopicPartitionInfo info = described(context, tp);
        refuseMoving(id, reassignments(context, Set.of(tp)).get(tp));
        if (preferred(info)) {
            return OperationResult.already();
        }
        try {
            context.await()
                    .get(context.admin()
                            .electLeaders(
                                    ElectionType.PREFERRED,
                                    Set.of(tp),
                                    new ElectLeadersOptions()
                                            .timeoutMs(context.await().timeoutMs()))
                            .all());
        } catch (BrokerException e) {
            noteUnsupported(context, KafkaAccess.PREFERRED_ELECTION, e);
            throw e;
        }
        context.topology().invalidate();
        return OperationResult.done();
    }

    static String electionEtag(Context context, ResourceId id) {
        TopicPartitionInfo info = described(context, Partitions.partition(id));
        return Etags.of(leader(info), replicaIds(info));
    }

    static String reassignmentEtag(Context context, ResourceId id) {
        TopicPartition tp = Partitions.partition(id);
        TopicPartitionInfo info = described(context, tp);
        return Etags.of(
                replicaIds(info),
                info.isr().stream().map(Node::id).toList(),
                leader(info),
                reassignmentId(reassignments(context, Set.of(tp)).get(tp)));
    }

    static Preview previewReassign(Context context, ResourceId id, Operation.Reassign operation) {
        List<Integer> target = assignment(id, operation);
        TopicPartition tp = Partitions.partition(id);
        TopicPartitionInfo info = described(context, tp);
        PartitionReassignment moving = reassignments(context, Set.of(tp)).get(tp);
        if (target.isEmpty()) {
            return cancelPreview(id, info, moving);
        }
        long rate = throttle(operation.options());
        checkFactor(target.size(), minInSync(context, tp.topic()));
        List<Integer> current = replicaIds(info);
        Count bytes = bytesToCopy(current, target, logs(context, info), tp, info.leader());
        List<AttributeChange> diff = new ArrayList<>();
        diff.add(new AttributeChange(KafkaKinds.REPLICAS, Optional.of(current), Optional.of(target)));
        diff.add(new AttributeChange(
                KafkaKinds.REPLICATION_FACTOR, Optional.of((long) current.size()), Optional.of((long) target.size())));
        diff.add(new AttributeChange(KafkaKinds.REPLICATION_THROTTLE, Optional.empty(), Optional.of(rate)));
        String note = current.equals(target)
                ? "The replicas are already in this order; nothing would be sent."
                : duration(bytes, rate);
        diff.add(new AttributeChange(KafkaKinds.NOTE, Optional.empty(), Optional.of(note)));
        return preview(diff, new Estimate(bytes, "By"), etag(info, moving));
    }

    /** Applies the move, or cancels when the replica list is empty. The returned attributes are the throttle baseline. */
    static Applied apply(Context context, ResourceId id, Operation.Reassign operation) {
        List<Integer> target = assignment(id, operation);
        TopicPartition tp = Partitions.partition(id);
        require(context, KafkaAccess.REASSIGNMENT);
        TopicPartitionInfo info = described(context, tp);
        PartitionReassignment moving = reassignments(context, Set.of(tp)).get(tp);
        if (target.isEmpty()) {
            cancel(context, id, tp, moving);
            return Applied.changed(Attributes.empty());
        }
        long rate = throttle(operation.options());
        checkFactor(target.size(), minInSync(context, tp.topic()));
        if (replicaIds(info).equals(target)) {
            return Applied.unchanged();
        }
        List<ThrottleKey> keys = throttleKeys(context, tp, info, target, rate);
        List<ThrottleWrite> writes = keys.stream()
                .map(ReplicaMoves::change)
                .flatMap(Optional::stream)
                .toList();
        if (!writes.isEmpty()) {
            alter(context, sets(writes));
        }
        try {
            alterReassignment(context, tp, Optional.of(new NewPartitionReassignment(target)));
        } catch (BrokerException e) {
            restoreAfter(context, writes, e);
            noteUnsupported(context, KafkaAccess.REASSIGNMENT, e);
            throw e;
        }
        context.topology().invalidate();
        return Applied.changed(throttleAttributes(keys));
    }

    record Applied(boolean alreadyInState, Attributes throttle) {
        static Applied unchanged() {
            return new Applied(true, Attributes.empty());
        }

        static Applied changed(Attributes throttle) {
            return new Applied(false, throttle);
        }
    }

    /**
     * Bytes to copy onto replicas the new set adds: the leader's size, once each. A missing size is unavailable, never
     * zero.
     */
    static Count bytesToCopy(
            List<Integer> current,
            List<Integer> target,
            ReplicaLogs.Read logs,
            TopicPartition partition,
            @Nullable Node leader) {
        List<Integer> added = new ArrayList<>();
        for (int broker : target) {
            if (!current.contains(broker)) {
                added.add(broker);
            }
        }
        if (added.isEmpty()) {
            return Count.estimate(0);
        }
        if (leader == null) {
            return Count.unavailable("The partition has no leader, so the bytes to copy are not known.");
        }
        ReplicaInfo info = logs.current(leader.id(), partition);
        if (info == null) {
            return Count.unavailable(
                    logs.unavailable().isBlank() ? "The leader's replica size is not known." : logs.unavailable());
        }
        try {
            return Count.estimate(Math.multiplyExact(info.size(), added.size()));
        } catch (ArithmeticException e) {
            return Count.unavailable("The bytes to copy do not fit in a count.");
        }
    }

    static String duration(Count bytes, long rate) {
        if (bytes instanceof Count.Unavailable unavailable) {
            return "The duration is unavailable: " + unavailable.reason();
        }
        long amount = bytes instanceof Count.Estimate estimate ? estimate.value() : ((Count.Exact) bytes).value();
        long seconds = (amount + rate - 1) / rate;
        return "About " + seconds + "s to copy " + amount + " bytes at " + rate + " bytes/s (estimate).";
    }

    static long throttle(Attributes options) {
        Object raw = options.raw(THROTTLE).orElse(null);
        Long rate = null;
        if (raw instanceof Number number) {
            rate = number.longValue();
        } else if (raw != null) {
            try {
                rate = Long.parseLong(String.valueOf(raw));
            } catch (NumberFormatException e) {
                rate = null;
            }
        }
        if (rate == null || rate <= 0) {
            throw refused("A reassignment needs " + THROTTLE + " as a positive whole number of bytes per second.");
        }
        return rate;
    }

    static void checkFactor(int factor, int minInSync) {
        if (factor < minInSync) {
            throw refused("Replication factor " + factor + " is below min.insync.replicas " + minInSync + ".");
        }
    }

    /** The replica list for this partition, or empty when the caller is cancelling. Any other shape is refused. */
    static List<Integer> assignment(ResourceId id, Operation.Reassign operation) {
        TopicPartition tp = Partitions.partition(id);
        PartitionId key = PartitionId.of(String.valueOf(tp.partition()));
        if (operation.assignments().size() != 1 || !operation.assignments().containsKey(key)) {
            throw refused("A reassignment of " + id + " names that partition and no other.");
        }
        List<Integer> brokers = new ArrayList<>();
        for (NodeId node : operation.assignments().get(key)) {
            int broker;
            try {
                broker = Integer.parseInt(node.value());
            } catch (NumberFormatException e) {
                throw Attrs.invalid("A replica is a broker number, not " + node.value() + ".");
            }
            if (brokers.contains(broker)) {
                throw Attrs.invalid("Broker " + broker + " is named twice.");
            }
            brokers.add(broker);
        }
        return brokers;
    }

    private static void cancel(
            Context context, ResourceId id, TopicPartition tp, @Nullable PartitionReassignment moving) {
        if (moving == null) {
            throw refused(id + " has no reassignment to cancel.");
        }
        try {
            alterReassignment(context, tp, Optional.empty());
        } catch (BrokerException e) {
            noteUnsupported(context, KafkaAccess.REASSIGNMENT, e);
            throw e;
        }
        context.topology().invalidate();
    }

    private static Preview cancelPreview(
            ResourceId id, TopicPartitionInfo info, @Nullable PartitionReassignment moving) {
        if (moving == null) {
            throw refused(id + " has no reassignment to cancel.");
        }
        List<AttributeChange> diff = new ArrayList<>();
        diff.add(new AttributeChange(
                KafkaKinds.REPLICAS, Optional.of(replicaIds(info)), Optional.of(withoutAdding(moving))));
        diff.add(new AttributeChange(KafkaKinds.REASSIGNMENT, Optional.of(reassignmentId(moving)), Optional.empty()));
        diff.add(new AttributeChange(
                KafkaKinds.NOTE, Optional.empty(), Optional.of("Cancels the in-progress reassignment.")));
        return preview(diff, new Estimate(Count.exact(0), "{replica}"), etag(info, moving));
    }

    private static List<Integer> withoutAdding(PartitionReassignment moving) {
        List<Integer> restored = new ArrayList<>();
        for (int replica : moving.replicas()) {
            if (!moving.addingReplicas().contains(replica)) {
                restored.add(replica);
            }
        }
        return restored;
    }

    private static void refuseMoving(ResourceId id, @Nullable PartitionReassignment moving) {
        if (moving != null) {
            throw refused(id + " has a reassignment in progress (" + reassignmentId(moving)
                    + "); electing a leader now is stale.");
        }
    }

    private static boolean preferred(TopicPartitionInfo info) {
        return info.leader() != null
                && !info.replicas().isEmpty()
                && info.leader().id() == info.replicas().getFirst().id();
    }

    private static Preview electionPreview(ResourceId id, TopicPartitionInfo info) {
        if (info.replicas().isEmpty()) {
            throw refused(id + " has no replicas to elect.");
        }
        boolean already = preferred(info);
        List<AttributeChange> diff = new ArrayList<>();
        int first = info.replicas().getFirst().id();
        if (!already && info.leader() != null) {
            diff.add(new AttributeChange(
                    KafkaKinds.LEADER, Optional.of((long) info.leader().id()), Optional.of((long) first)));
        }
        String note = already
                ? "Already the preferred leader; nothing would be sent."
                : "The leader would move from " + leader(info) + " to " + first + ", the first replica.";
        diff.add(new AttributeChange(KafkaKinds.NOTE, Optional.empty(), Optional.of(note)));
        return new Preview(
                diff,
                Set.of(),
                new Estimate(Count.exact(already ? 0 : 1), "{leader}"),
                Etags.of(leader(info), replicaIds(info)),
                EtagBasis.CONFIGURATION,
                PreconditionEnforcement.CHECKED_BEFORE);
    }

    private static Preview preview(List<AttributeChange> diff, Estimate estimate, String etag) {
        return new Preview(
                diff, Set.of(), estimate, etag, EtagBasis.CONFIGURATION, PreconditionEnforcement.CHECKED_BEFORE);
    }

    private static String etag(TopicPartitionInfo info, @Nullable PartitionReassignment moving) {
        return Etags.of(
                replicaIds(info), info.isr().stream().map(Node::id).toList(), leader(info), reassignmentId(moving));
    }

    private static List<ThrottleKey> throttleKeys(
            Context context, TopicPartition tp, TopicPartitionInfo info, List<Integer> target, long rate) {
        List<Integer> current = replicaIds(info);
        List<Integer> added = new ArrayList<>();
        for (int broker : target) {
            if (!current.contains(broker)) {
                added.add(broker);
            }
        }
        Set<ConfigResource> resources = new LinkedHashSet<>();
        for (int broker : target) {
            resources.add(new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(broker)));
        }
        ConfigResource topic = new ConfigResource(ConfigResource.Type.TOPIC, tp.topic());
        resources.add(topic);
        Map<ConfigResource, Config> configs = describe(context, resources);
        List<ThrottleKey> keys = new ArrayList<>();
        for (int broker : target) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(broker));
            Config config = configs.get(resource);
            keys.add(key(resource, LEADER_RATE, Long.toString(rate), config));
            keys.add(key(resource, FOLLOWER_RATE, Long.toString(rate), config));
        }
        Config topicConfig = configs.get(topic);
        keys.add(key(
                topic,
                LEADER_REPLICAS,
                merge(override(topicConfig, LEADER_REPLICAS).orElse(null), tp.partition(), current),
                topicConfig));
        keys.add(key(
                topic,
                FOLLOWER_REPLICAS,
                merge(override(topicConfig, FOLLOWER_REPLICAS).orElse(null), tp.partition(), added),
                topicConfig));
        return keys;
    }

    private static ThrottleKey key(ConfigResource resource, String name, String desired, @Nullable Config config) {
        return new ThrottleKey(resource, name, override(config, name), desired);
    }

    /** The dynamic override, or empty when the value is only the built-in default. */
    static Optional<String> override(@Nullable Config config, String name) {
        if (config == null) {
            return Optional.empty();
        }
        ConfigEntry entry = config.get(name);
        if (entry == null || entry.value() == null || entry.source() == ConfigEntry.ConfigSource.DEFAULT_CONFIG) {
            return Optional.empty();
        }
        return Optional.of(entry.value());
    }

    /** This partition's brokers merged into the throttled-replica list. A {@code *} list is left as it is. */
    static String merge(@Nullable String current, int partition, List<Integer> brokers) {
        if (current != null && current.trim().equals("*")) {
            return "*";
        }
        Set<String> entries = new TreeSet<>();
        if (current != null && !current.isBlank()) {
            for (String part : current.split(",")) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) {
                    entries.add(trimmed);
                }
            }
        }
        for (int broker : brokers) {
            entries.add(partition + ":" + broker);
        }
        return String.join(",", entries);
    }

    private static Map<ConfigResource, Collection<AlterConfigOp>> sets(List<ThrottleWrite> writes) {
        Map<ConfigResource, List<AlterConfigOp>> out = new LinkedHashMap<>();
        for (ThrottleWrite write : writes) {
            out.computeIfAbsent(write.resource(), ignored -> new ArrayList<>())
                    .add(new AlterConfigOp(new ConfigEntry(write.key(), write.applied()), AlterConfigOp.OpType.SET));
        }
        return new LinkedHashMap<>(out);
    }

    private static void restoreAfter(Context context, List<ThrottleWrite> writes, BrokerException failure) {
        if (writes.isEmpty()) {
            return;
        }
        try {
            alter(context, restore(writes));
        } catch (RuntimeException restoreFailure) {
            failure.addSuppressed(restoreFailure);
        }
    }

    private static void alter(Context context, Map<ConfigResource, Collection<AlterConfigOp>> ops) {
        if (ops.isEmpty()) {
            return;
        }
        context.await()
                .get(context.admin()
                        .incrementalAlterConfigs(
                                ops,
                                new AlterConfigsOptions()
                                        .timeoutMs(context.await().timeoutMs()))
                        .all());
    }

    private static void alterReassignment(
            Context context, TopicPartition tp, Optional<NewPartitionReassignment> assignment) {
        context.await()
                .get(context.admin()
                        .alterPartitionReassignments(
                                Map.of(tp, assignment),
                                new AlterPartitionReassignmentsOptions()
                                        .allowReplicationFactorChange(true)
                                        .timeoutMs(context.await().timeoutMs()))
                        .values()
                        .get(tp));
    }

    private static Map<ConfigResource, Config> describe(Context context, Set<ConfigResource> resources) {
        return context.await()
                .get(context.admin()
                        .describeConfigs(
                                resources,
                                new DescribeConfigsOptions()
                                        .timeoutMs(context.await().timeoutMs()))
                        .all());
    }

    private static int minInSync(Context context, String topic) {
        Config config = describe(context, Set.of(new ConfigResource(ConfigResource.Type.TOPIC, topic)))
                .get(new ConfigResource(ConfigResource.Type.TOPIC, topic));
        Optional<String> value = override(config, MIN_IN_SYNC);
        if (value.isEmpty()) {
            ConfigEntry entry = config == null ? null : config.get(MIN_IN_SYNC);
            if (entry == null || entry.value() == null || entry.value().isBlank()) {
                return 1;
            }
            value = Optional.of(entry.value());
        }
        try {
            return Integer.parseInt(value.get());
        } catch (NumberFormatException e) {
            return 1;
        }
    }

    private static ReplicaLogs.Read logs(Context context, TopicPartitionInfo info) {
        Set<Integer> brokers = new LinkedHashSet<>();
        for (Node replica : info.replicas()) {
            if (replica.id() >= 0) {
                brokers.add(replica.id());
            }
        }
        if (brokers.isEmpty()) {
            return new ReplicaLogs.Read(Map.of(), "");
        }
        return ReplicaLogs.read(
                context.await(),
                context.admin()
                        .describeLogDirs(
                                brokers,
                                new DescribeLogDirsOptions()
                                        .timeoutMs(context.await().timeoutMs()))
                        .descriptions());
    }

    private static TopicPartitionInfo described(Context context, TopicPartition tp) {
        TopicDescription description = context.await()
                .get(context.admin()
                        .describeTopics(
                                TopicCollection.ofTopicNames(List.of(tp.topic())),
                                new DescribeTopicsOptions()
                                        .timeoutMs(context.await().timeoutMs()))
                        .topicNameValues()
                        .get(tp.topic()));
        return description.partitions().stream()
                .filter(info -> info.partition() == tp.partition())
                .findFirst()
                .orElseThrow(() -> Handler.gone(Partitions.id(tp.topic(), tp.partition())));
    }

    private static List<Integer> replicaIds(TopicPartitionInfo info) {
        return info.replicas().stream().map(Node::id).toList();
    }

    private static String leader(TopicPartitionInfo info) {
        return info.leader() == null ? "none" : Integer.toString(info.leader().id());
    }

    private static BrokerException refused(String message) {
        return KafkaErrors.refused(Status.FAILED_PRECONDITION, ErrorReasons.PRECONDITION_FAILED, message);
    }
}
