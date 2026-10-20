package io.github.sudoitir.openbroker.model;

import io.github.sudoitir.openbroker.Partial;
import java.util.List;

/** The cluster's nodes. Nodes are not resources; placements and shards refer to them by id. */
public interface Nodes {

    /**
     * Describes the nodes in scope, calling each one; a node that does not answer is a failure in
     * the result, not an exception. Describing one node at a time keeps a slow node from delaying
     * the others.
     */
    Partial<List<NodeDescription>> describe(Scope scope);

    /**
     * The cluster's members as each node in scope knows them, including members this connection has
     * not been given: how a caller discovers a cluster from a seed. Each node's answer is listed in
     * scope order.
     */
    Partial<List<ClusterMember>> members(Scope scope);
}
