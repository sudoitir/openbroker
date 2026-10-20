package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.artemis.JolokiaResponse;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.ClusterMember;
import io.github.sudoitir.openbroker.model.Condition;
import io.github.sudoitir.openbroker.model.ConditionTypes;
import io.github.sudoitir.openbroker.model.Designation;
import io.github.sudoitir.openbroker.model.Membership;
import io.github.sudoitir.openbroker.model.NodeDescription;
import io.github.sudoitir.openbroker.model.NodeFunction;
import io.github.sudoitir.openbroker.model.Nodes;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;

/**
 * Describes nodes with one read of the broker MBean's HA attributes per node: whether it is active,
 * started, a backup, in sync, its NodeID, whether it is clustered, and its version.
 */
public final class ArtemisNodes implements Nodes {

    /** Native attributes a description carries, namespaced. */
    public static final String ACTIVE = "artemis.active";

    public static final String STARTED = "artemis.started";
    public static final String BACKUP = "artemis.backup";
    public static final String CLUSTERED = "artemis.clustered";
    public static final String REPLICA_SYNC = "artemis.replica_sync";

    /** Native attributes of a cluster member: the connectors {@code listNetworkTopology} reports. */
    public static final String LIVE = "artemis.live_connector";

    public static final String PRIMARY = "artemis.primary_connector";
    public static final String BACKUP_CONNECTOR = "artemis.backup_connector";

    private final Cluster cluster;

    public ArtemisNodes(Cluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public Partial<List<NodeDescription>> describe(Scope scope) {
        Deadline deadline = Deadlines.current(cluster.options());
        return Fanout.each(cluster.nodes(scope), cluster.options(), node -> List.of(describe(node, deadline)));
    }

    @Override
    public Partial<List<ClusterMember>> members(Scope scope) {
        Deadline deadline = Deadlines.current(cluster.options());
        return Fanout.each(cluster.nodes(scope), cluster.options(), node -> {
            JsonNode topology = cluster.jolokia(node, deadline).execOnBrokerParsed("listNetworkTopology()");
            List<ClusterMember> members = new ArrayList<>();
            if (topology != null && topology.isArray()) {
                for (JsonNode e : topology) {
                    members.add(member(e));
                }
            }
            return members;
        });
    }

    /** A {@code listNetworkTopology} entry: older brokers name the serving connector {@code live}. */
    private static ClusterMember member(JsonNode e) {
        String live = Ha.text(e, "live");
        String primary = Ha.text(e, "primary");
        String backup = Ha.text(e, "backup");
        List<ClusterMember.Replica> replicas = new ArrayList<>();
        String serving = primary != null ? primary : live;
        if (serving != null) {
            replicas.add(
                    new ClusterMember.Replica(Designation.PRIMARY, new Endpoint(Plane.DATA, NodeHandle.CORE, serving)));
        }
        if (backup != null) {
            replicas.add(
                    new ClusterMember.Replica(Designation.BACKUP, new Endpoint(Plane.DATA, NodeHandle.CORE, backup)));
        }
        Attributes.Builder native_ = Attributes.builder();
        if (live != null) {
            native_.putRaw(LIVE, live);
        }
        if (primary != null) {
            native_.putRaw(PRIMARY, primary);
        }
        if (backup != null) {
            native_.putRaw(BACKUP_CONNECTOR, backup);
        }
        String id = Ha.text(e, "nodeID");
        return new ClusterMember(id == null ? "" : id, replicas, native_.build());
    }

    private NodeDescription describe(NodeHandle node, Deadline deadline) {
        JolokiaClient client = cluster.jolokia(node);
        JolokiaResponse response = client.readBrokerAttributes(deadline, Ha.ATTRIBUTES);
        if (!response.ok()) {
            // The broker answered, with an error: a failed observation, not an unreachable node.
            throw Errors.of(
                    Status.INTERNAL,
                    ErrorReasons.BAD_RESPONSE,
                    Dispatch.REJECTED,
                    node.id(),
                    "The HA read answered with an error: " + response.failure(),
                    Map.of(Errors.NATIVE_ERROR, response.failure()));
        }
        JsonNode ha = response.value();
        Boolean active = Ha.bool(ha, "Active");
        Boolean started = Ha.bool(ha, "Started");
        Boolean backup = Ha.bool(ha, "Backup");
        Boolean clustered = Ha.bool(ha, "Clustered");
        Boolean replicaSync = Ha.bool(ha, "ReplicaSync");
        Attributes.Builder attributes = Attributes.builder();
        putIfPresent(attributes, ACTIVE, active);
        putIfPresent(attributes, STARTED, started);
        putIfPresent(attributes, BACKUP, backup);
        putIfPresent(attributes, CLUSTERED, clustered);
        putIfPresent(attributes, REPLICA_SYNC, replicaSync);
        List<Endpoint> endpoints = new ArrayList<>();
        node.jolokia()
                .ifPresent(uri -> endpoints.add(new Endpoint(Plane.MANAGEMENT, NodeHandle.JOLOKIA, uri.toString())));
        return new NodeDescription(
                node.id(),
                Optional.ofNullable(Ha.text(ha, "NodeID")).orElse(""),
                new Membership(
                        Optional.empty(),
                        Ha.designation(backup, clustered),
                        Ha.serving(active),
                        Optional.ofNullable(replicaSync)),
                Set.of(NodeFunction.BROKER),
                endpoints,
                Optional.ofNullable(Ha.text(ha, "Version")),
                List.of(new Condition(ConditionTypes.STARTED, Ha.started(started), "", "", Optional.empty())),
                attributes.build(),
                Optional.ofNullable(response.timestamp()).map(Instant::ofEpochSecond));
    }

    private static void putIfPresent(Attributes.Builder builder, String name, @Nullable Boolean value) {
        if (value != null) {
            builder.putRaw(name, value);
        }
    }
}
