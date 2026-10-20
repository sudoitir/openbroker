package io.github.sudoitir.openbroker;

import io.github.sudoitir.openbroker.access.AccessSnapshot;
import io.github.sudoitir.openbroker.model.NodeDescription;
import java.util.List;

/**
 * What {@link BrokerDrivers#verify} learned without keeping a connection: each node that answered,
 * the failures of those that did not, and what the account may do.
 */
public record ConnectionReport(Partial<List<NodeDescription>> nodes, AccessSnapshot access) {}
