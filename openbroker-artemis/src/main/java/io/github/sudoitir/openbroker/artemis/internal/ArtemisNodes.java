package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.Partial;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Condition;
import io.github.sudoitir.openbroker.model.ConditionTypes;
import io.github.sudoitir.openbroker.model.Membership;
import io.github.sudoitir.openbroker.model.NodeDescription;
import io.github.sudoitir.openbroker.model.NodeFunction;
import io.github.sudoitir.openbroker.model.Nodes;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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

    private final Cluster cluster;

    public ArtemisNodes(Cluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public Partial<List<NodeDescription>> describe(Scope scope) {
        Deadline deadline = Deadlines.current(cluster.options());
        return Fanout.each(cluster.nodes(scope), cluster.options(), node -> List.of(describe(node, deadline)));
    }

    private NodeDescription describe(NodeHandle node, Deadline deadline) {
        JolokiaClient client = cluster.jolokia(node);
        JolokiaResponse response = client.readBrokerAttributes(deadline, Ha.ATTRIBUTES);
        if (!response.ok()) {
            throw Errors.badResponse(node.id(), "The HA read answered with an error: " + response.failure(), null);
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
        node.jolokia().ifPresent(uri -> endpoints.add(new Endpoint(Plane.MANAGEMENT, NodeHandle.JOLOKIA, uri.toString())));
        return new NodeDescription(
                node.id(),
                Optional.ofNullable(Ha.text(ha, "NodeID")).orElse(""),
                new Membership(Optional.empty(), Ha.designation(backup, clustered), Ha.serving(active),
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
