package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.access.Access;
import io.github.sudoitir.openbroker.access.AccessSnapshot;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.access.Capability;
import io.github.sudoitir.openbroker.artemis.ArtemisMBeans;
import io.github.sudoitir.openbroker.artemis.ArtemisRemedies;
import io.github.sudoitir.openbroker.artemis.JolokiaResponse;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * What the account may do, without ever changing the broker. A read is tried; a write is never tried
 * to find out, so management writes, browsing and sending are unknown until a real call settles them.
 * Notifications are unavailable when a node lacks what a subscription needs, and unknown otherwise.
 */
public final class ArtemisAccess implements Access {

    private static final String NOTIFICATIONS_ADDRESS = "activemq.notifications";

    private final Cluster cluster;

    public ArtemisAccess(Cluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public Assessment check(Capability capability, Optional<ResourceId> resource) {
        return probe().of(capability);
    }

    @Override
    public AccessSnapshot probe() {
        Deadline deadline = Deadlines.current(cluster.options());
        Map<Capability, Assessment> out = new LinkedHashMap<>();
        Assessment read = Assessment.unknown("No node of this cluster has a management endpoint.");
        NodeHandle reached = null;
        for (NodeHandle node : cluster.nodes()) {
            if (node.jolokia().isEmpty()) {
                continue;
            }
            read = read(node, deadline);
            if (read.status() == Assessment.Status.AVAILABLE) {
                reached = node;
                break;
            }
        }
        out.put(Capability.MANAGEMENT_READ, read);
        if (reached == null) {
            Assessment unknown = Assessment.unknown("Not assessed: management reads are not available.");
            List.of(Capability.MANAGEMENT_WRITE, Capability.NOTIFICATIONS, Capability.BROWSE, Capability.SEND)
                    .forEach(c -> out.put(c, unknown));
            return new AccessSnapshot(out, cluster.options().clock().instant());
        }
        out.put(
                Capability.MANAGEMENT_WRITE,
                new Assessment(
                        Assessment.Status.UNKNOWN,
                        "A management write is never tried to find out. A read succeeding does not establish it:"
                                + " Jolokia can allow single operations, and the broker's manage permission is a"
                                + " separate gate. The next write settles it.",
                        Optional.of(ArtemisRemedies.MANAGEMENT_SECURITY)));
        out.put(
                Capability.BROWSE,
                Assessment.unknown(
                        "Browsing is a management operation the broker may restrict; the first browse settles it."));
        out.put(
                Capability.SEND,
                Assessment.unknown("Sending needs the send permission on the address; the first send settles it."));
        out.put(Capability.NOTIFICATIONS, notifications(reached, deadline));
        return new AccessSnapshot(out, cluster.options().clock().instant());
    }

    private Assessment read(NodeHandle node, Deadline deadline) {
        try {
            JolokiaResponse response = cluster.jolokia(node).readBrokerAttributes(deadline, "Version");
            return response.ok()
                    ? Assessment.available("A read of the broker's attributes succeeded.")
                    : Assessment.unavailable(
                            "The broker refused a read of its attributes: " + response.failure(), Optional.empty());
        } catch (BrokerException e) {
            if (ErrorReasons.CREDENTIALS_REJECTED.equals(e.reason())) {
                return Assessment.unavailable(
                        "The broker rejected these credentials for a management read.",
                        Optional.of(ArtemisRemedies.MANAGEMENT_SECURITY));
            }
            return Assessment.unknown("The node could not be read: " + e.getMessage());
        }
    }

    /** What a node shows of a subscription's preconditions; only subscribing proves the rest. */
    private Assessment notifications(NodeHandle node, Deadline deadline) {
        if (node.core().isEmpty()) {
            return Assessment.unavailable(
                    "This node has no Core endpoint, which a subscription needs.",
                    Optional.of(ArtemisRemedies.CORE_ACCEPTOR));
        }
        try {
            JolokiaClient client = cluster.jolokia(node);
            String broker = client.brokerName(deadline);
            if (client.search(ArtemisMBeans.address(broker, NOTIFICATIONS_ADDRESS), deadline)
                    .isEmpty()) {
                return Assessment.unavailable(
                        "The broker has no " + NOTIFICATIONS_ADDRESS + " address.", Optional.empty());
            }
        } catch (BrokerException e) {
            return Assessment.unknown("The node could not be read: " + e.getMessage());
        }
        return new Assessment(
                Assessment.Status.UNKNOWN,
                "A Core endpoint and the " + NOTIFICATIONS_ADDRESS + " address are present; subscribing settles"
                        + " whether the account may consume from it.",
                Optional.of(ArtemisRemedies.NOTIFICATIONS_SECURITY));
    }
}
