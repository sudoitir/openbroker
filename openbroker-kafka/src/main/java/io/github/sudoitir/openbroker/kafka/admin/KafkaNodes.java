package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeOutcome;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.KafkaOptions;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.ClusterMember;
import io.github.sudoitir.openbroker.model.Condition;
import io.github.sudoitir.openbroker.model.ConditionStatus;
import io.github.sudoitir.openbroker.model.ConditionTypes;
import io.github.sudoitir.openbroker.model.Designation;
import io.github.sudoitir.openbroker.model.Membership;
import io.github.sudoitir.openbroker.model.NodeDescription;
import io.github.sudoitir.openbroker.model.NodeFunction;
import io.github.sudoitir.openbroker.model.Nodes;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.Serving;
import io.github.sudoitir.openbroker.model.Shard;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumOptions;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.RaftVoterEndpoint;
import org.apache.kafka.common.Node;
import org.jspecify.annotations.Nullable;

/**
 * Brokers are the nodes, from {@code describeCluster}. The controller quorum, from {@code
 * describeMetadataQuorum}, marks the nodes that vote, adds the controllers that are not brokers,
 * and says whether each voter keeps up with the leader. Partitions that list a broker as a replica
 * out of the in-sync set make the broker under-replicated.
 */
public final class KafkaNodes implements Nodes {

    /** Native attributes of a node, namespaced. */
    public static final String CLUSTER_ID = "kafka.cluster_id";

    public static final String RACK = "kafka.rack";
    public static final String QUORUM_VOTER = "kafka.quorum_voter";
    public static final String QUORUM_LEADER = "kafka.quorum_leader";
    public static final String QUORUM_LAG = "kafka.quorum_lag";
    public static final String OUT_OF_SYNC_PARTITIONS = "kafka.out_of_sync_partitions";

    /** The protocol of a controller listener endpoint. */
    public static final String CONTROLLER_PROTOCOL = "kafka-controller";

    /** A voter that last caught up with the leader longer ago than this is out of sync. */
    static final Duration VOTER_LAG_TOLERANCE = Duration.ofSeconds(10);

    private final KafkaCluster cluster;
    private final Topology topology;

    public KafkaNodes(KafkaCluster cluster, Topology topology) {
        this.cluster = cluster;
        this.topology = topology;
    }

    @Override
    public Partial<List<NodeDescription>> describe(Scope scope) {
        Deadline deadline = Deadlines.current(cluster.options());
        Snapshot snapshot = cluster.call("nodes.describe", Optional.empty(), deadline, this::snapshot);
        return select(scope, snapshot.descriptions(), d -> d.node());
    }

    @Override
    public Partial<List<ClusterMember>> members(Scope scope) {
        Deadline deadline = Deadlines.current(cluster.options());
        Snapshot snapshot = cluster.call("nodes.members", Optional.empty(), deadline, this::snapshot);
        List<ClusterMember> members = new ArrayList<>();
        for (Node broker : snapshot.brokers()) {
            ClusterMember member = new ClusterMember(
                    broker.idString(),
                    List.of(new ClusterMember.Replica(Designation.NONE, brokerEndpoint(broker))),
                    Attributes.empty());
            members.add(member);
        }
        return select(scope, members, m -> NodeId.of(m.nativeId()));
    }

    /** The items of the nodes a scope names, with a failure for each named node the cluster lacks. */
    private static <T> Partial<List<T>> select(Scope scope, List<T> all, Function<T, NodeId> idOf) {
        if (scope instanceof Scope.Whole) {
            return Partial.complete(all, all.stream().map(idOf).toList());
        }
        List<T> selected = new ArrayList<>();
        Map<NodeId, NodeOutcome> outcomes = new LinkedHashMap<>();
        Map<NodeId, BrokerException> failures = new LinkedHashMap<>();
        for (Shard shard : ((Scope.Shards) scope).shards()) {
            if (!(shard instanceof Shard.NodeInstance instance)) {
                throw KafkaErrors.refused(
                        Status.INVALID_ARGUMENT,
                        ErrorReasons.INVALID_ARGUMENT,
                        "Scope node work to node instances; partitions are resources, not nodes.");
            }
            Optional<T> found = all.stream()
                    .filter(i -> idOf.apply(i).equals(instance.node()))
                    .findFirst();
            if (found.isPresent()) {
                selected.add(found.get());
                outcomes.put(instance.node(), NodeOutcome.APPLIED);
            } else {
                outcomes.put(instance.node(), NodeOutcome.FAILED);
                failures.put(
                        instance.node(),
                        new BrokerException(
                                Status.NOT_FOUND,
                                ErrorInfo.of(ErrorReasons.INVALID_ARGUMENT),
                                Dispatch.NOT_SENT,
                                instance.node(),
                                "The cluster has no node " + instance.node() + "."));
            }
        }
        return new Partial<>(selected, outcomes, failures);
    }

    private Snapshot snapshot(Admin admin, Await await) {
        DescribeClusterResult described =
                admin.describeCluster(new DescribeClusterOptions().timeoutMs(await.timeoutMs()));
        Collection<Node> brokers = await.get(described.nodes());
        String clusterId = await.get(described.clusterId());
        Quorum quorum = quorum(admin, await);
        Topology.Snapshot topics = topology.read(admin, await);
        return new Snapshot(
                List.copyOf(brokers), clusterId, quorum, Replication.outOfSyncPartitions(topics.topics()), topics);
    }

    private static Quorum quorum(Admin admin, Await await) {
        try {
            return new Quorum(
                    await.get(admin.describeMetadataQuorum(
                                    new DescribeMetadataQuorumOptions().timeoutMs(await.timeoutMs()))
                            .quorumInfo()),
                    null);
        } catch (BrokerException e) {
            if (e.status() == Status.DEADLINE_EXCEEDED) {
                throw e;
            }
            return new Quorum(null, e.getMessage());
        }
    }

    private static Endpoint brokerEndpoint(Node broker) {
        return new Endpoint(Plane.DATA, KafkaOptions.PROTOCOL, broker.host() + ":" + broker.port());
    }

    /** What one round of reads found. */
    private record Snapshot(
            List<Node> brokers,
            String clusterId,
            Quorum quorum,
            Map<Integer, Long> outOfSync,
            Topology.Snapshot topics) {

        List<NodeDescription> descriptions() {
            List<NodeDescription> out = new ArrayList<>();
            for (Node broker : brokers) {
                out.add(broker(broker));
            }
            QuorumInfo info = quorum.info();
            if (info != null) {
                Set<Integer> brokerIds = new HashSet<>();
                brokers.forEach(b -> brokerIds.add(b.id()));
                for (QuorumInfo.ReplicaState voter : info.voters()) {
                    if (!brokerIds.contains(voter.replicaId())) {
                        out.add(controller(info, voter));
                    }
                }
            }
            return out;
        }

        private NodeDescription broker(Node broker) {
            Set<NodeFunction> functions = EnumSet.of(NodeFunction.BROKER);
            Attributes.Builder attributes = Attributes.builder().putRaw(CLUSTER_ID, clusterId);
            if (broker.hasRack()) {
                attributes.putRaw(RACK, broker.rack());
            }
            long lagging = outOfSync.getOrDefault(broker.id(), 0L);
            attributes.putRaw(OUT_OF_SYNC_PARTITIONS, lagging);
            List<Condition> conditions = new ArrayList<>();
            conditions.add(new Condition(
                    ConditionTypes.SERVING,
                    broker.isFenced() ? ConditionStatus.FALSE : ConditionStatus.TRUE,
                    broker.isFenced() ? "FENCED" : "",
                    broker.isFenced() ? "The controller has fenced this broker, so it serves no clients." : "",
                    Optional.empty()));
            conditions.add(new Condition(
                    ConditionTypes.UNDER_REPLICATED,
                    lagging > 0 ? ConditionStatus.TRUE : ConditionStatus.FALSE,
                    lagging > 0 ? "OUT_OF_SYNC_REPLICAS" : "",
                    underReplicationMessage(lagging),
                    Optional.empty()));
            Optional<Boolean> inSync = Optional.empty();
            QuorumInfo info = quorum.info();
            if (info == null) {
                conditions.add(quorumUnknown());
            } else {
                QuorumInfo.ReplicaState voter = voter(info, broker.id());
                if (voter != null) {
                    functions.add(NodeFunction.CONTROLLER);
                    inSync = voterState(info, voter, attributes, conditions);
                }
            }
            return new NodeDescription(
                    NodeId.of(broker.idString()),
                    broker.idString(),
                    new Membership(
                            Optional.empty(),
                            Designation.NONE,
                            broker.isFenced() ? Serving.STANDBY : Serving.ACTIVE,
                            inSync),
                    functions,
                    List.of(brokerEndpoint(broker)),
                    Optional.empty(),
                    conditions,
                    attributes.build(),
                    Optional.empty());
        }

        private NodeDescription controller(QuorumInfo info, QuorumInfo.ReplicaState voter) {
            Attributes.Builder attributes = Attributes.builder().putRaw(CLUSTER_ID, clusterId);
            List<Condition> conditions = new ArrayList<>();
            Optional<Boolean> inSync = voterState(info, voter, attributes, conditions);
            QuorumInfo.Node node = info.nodes().get(voter.replicaId());
            List<Endpoint> endpoints = new ArrayList<>();
            if (node != null) {
                for (RaftVoterEndpoint e : node.endpoints()) {
                    endpoints.add(new Endpoint(Plane.MANAGEMENT, CONTROLLER_PROTOCOL, e.host() + ":" + e.port()));
                }
            }
            return new NodeDescription(
                    NodeId.of(String.valueOf(voter.replicaId())),
                    String.valueOf(voter.replicaId()),
                    new Membership(Optional.empty(), Designation.NONE, Serving.ACTIVE, inSync),
                    EnumSet.of(NodeFunction.CONTROLLER),
                    endpoints,
                    Optional.empty(),
                    conditions,
                    attributes.build(),
                    Optional.empty());
        }

        /** Records a voter's place in the quorum; whether it keeps up with the leader, when known. */
        private static Optional<Boolean> voterState(
                QuorumInfo info,
                QuorumInfo.ReplicaState voter,
                Attributes.Builder attributes,
                List<Condition> conditions) {
            boolean leader = voter.replicaId() == info.leaderId();
            attributes.putRaw(QUORUM_VOTER, true).putRaw(QUORUM_LEADER, leader);
            QuorumInfo.ReplicaState head = voter(info, info.leaderId());
            if (head != null) {
                attributes.putRaw(QUORUM_LAG, Math.max(0, head.logEndOffset() - voter.logEndOffset()));
            }
            OptionalLong leaderCaughtUp = head == null ? OptionalLong.empty() : head.lastCaughtUpTimestamp();
            OptionalLong voterCaughtUp = voter.lastCaughtUpTimestamp();
            if (leader) {
                return Optional.of(true);
            }
            if (leaderCaughtUp.isEmpty() || voterCaughtUp.isEmpty()) {
                conditions.add(new Condition(
                        ConditionTypes.OUT_OF_SYNC,
                        ConditionStatus.UNKNOWN,
                        "NO_CATCH_UP_TIME",
                        "The quorum reports no time at which this voter caught up with the leader.",
                        Optional.empty()));
                return Optional.empty();
            }
            boolean behind = leaderCaughtUp.getAsLong() - voterCaughtUp.getAsLong() > VOTER_LAG_TOLERANCE.toMillis();
            conditions.add(new Condition(
                    ConditionTypes.OUT_OF_SYNC,
                    behind ? ConditionStatus.TRUE : ConditionStatus.FALSE,
                    behind ? "VOTER_BEHIND" : "",
                    behind
                            ? "This voter last caught up with the quorum leader more than "
                                    + VOTER_LAG_TOLERANCE.toSeconds()
                                    + " seconds before the leader's own latest."
                            : "",
                    Optional.empty()));
            return Optional.of(!behind);
        }

        /** Under-replication is read from the shared topic description, so it says as of when. */
        private String underReplicationMessage(long lagging) {
            return (lagging > 0 ? lagging + " partitions keep a replica on this broker that is not in sync" : "")
                    + (lagging > 0 ? ", as" : "As")
                    + " of "
                    + topics.takenAt()
                    + (topics.complete()
                            ? "."
                            : ", over the first " + topics.topics().size() + " of " + topics.totalTopics()
                                    + " topics.");
        }

        private Condition quorumUnknown() {
            return new Condition(
                    ConditionTypes.OUT_OF_SYNC,
                    ConditionStatus.UNKNOWN,
                    "QUORUM_UNAVAILABLE",
                    quorum.failure() == null ? "" : quorum.failure(),
                    Optional.empty());
        }

        private static QuorumInfo.@Nullable ReplicaState voter(QuorumInfo info, int id) {
            return info.voters().stream()
                    .filter(v -> v.replicaId() == id)
                    .findFirst()
                    .orElse(null);
        }
    }

    /** The controller quorum, or why it could not be read. */
    private record Quorum(
            @Nullable QuorumInfo info, @Nullable String failure) {}
}
