package io.github.sudoitir.openbroker.tck;

import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.NodeId;

/**
 * What a driver gives {@link ChaosTck}: requests to a test cluster whose nodes the TCK can cut off,
 * kill and start again, and a primary/backup pair that fails over.
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

    /** The node of {@link #haRequest()} that serves while the pair is healthy. */
    NodeId primary();

    /** The node of {@link #haRequest()} that takes over when the primary fails. */
    NodeId backup();

    /** Drops all traffic between the clients and the node, until {@link #heal}. */
    void cut(NodeId node);

    /** Restores the traffic {@link #cut} dropped. */
    void heal(NodeId node);

    /** Stops the node's process without a clean shutdown. */
    void kill(NodeId node);

    /** Starts a killed node again, with its data. */
    void start(NodeId node);
}
