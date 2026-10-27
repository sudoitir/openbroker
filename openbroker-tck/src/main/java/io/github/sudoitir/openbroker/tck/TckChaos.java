package io.github.sudoitir.openbroker.tck;

import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.model.ResourceId;

/**
 * What a driver gives {@link ChaosTck}: requests to a test cluster whose nodes the TCK can cut off,
 * kill and start again, and a cluster whose nodes fail over to each other.
 *
 * <p>The requests must use short timeouts (a second or two) so that a lost link is noticed within
 * seconds. Every method that changes a node returns once the change has taken effect. Cutting,
 * healing and starting are idempotent: healing a node that is not cut, or starting one that is
 * running, does nothing.
 */
public interface TckChaos {

    /** Reaches the single-node cluster, through the path {@link #cut} and {@link #heal} control. */
    ConnectRequest request();

    /**
     * Reaches the high-availability pair through its primary. The request names the primary as its
     * node and carries what the driver needs to find the backup.
     */
    ConnectRequest haRequest();

    /** The node of {@link #request()}. */
    NodeId node();

    /**
     * The node of {@link #haRequest()} that serves {@code subscription} while the cluster is healthy:
     * the member that holds its messages, such as a pair's live member or a partition's leader.
     * Resolved when called, so a subscription created after the request is covered.
     */
    NodeId serving(ResourceId subscription);

    /**
     * The node of {@link #haRequest()} that takes over {@code subscription} when {@link #serving} fails,
     * such as a pair's backup or an in-sync follower.
     */
    NodeId successor(ResourceId subscription);

    /**
     * Whether a link's events name the node that serves it, such as a pair's live member. A link on a
     * partitioned log is served by the cluster, not by one node, so its events name the cluster and
     * the case that expects the successor's name is skipped.
     */
    default boolean linksNameTheirServingNode() {
        return true;
    }

    /** Drops all traffic between the clients and the node, until {@link #heal}. */
    void cut(NodeId node);

    /** Restores the traffic {@link #cut} dropped. */
    void heal(NodeId node);

    /** Stops the node's process without a clean shutdown. */
    void kill(NodeId node);

    /** Starts a killed node again, with its data. */
    void start(NodeId node);
}
