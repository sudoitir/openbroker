package io.github.sudoitir.openbroker.artemis;

import io.github.sudoitir.openbroker.NodeId;

/**
 * What only Artemis can do, reached through {@code ClusterConnection#unwrap(ArtemisManagement.class)}:
 * broker.xml-level settings, configuration reads and the management operations that have no neutral
 * meaning. Code that uses it is Artemis-specific by definition; keep it in one place.
 */
public interface ArtemisManagement {

    /** The node's Jolokia agent. */
    ArtemisJolokia jolokia(NodeId node);
}
