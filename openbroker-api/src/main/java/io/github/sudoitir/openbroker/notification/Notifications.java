package io.github.sudoitir.openbroker.notification;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.model.Scope;
import java.util.Map;

/**
 * A stream of what happens on the brokers. A system with no push notifications offers them by
 * polling, through the support library.
 */
public interface Notifications {

    /**
     * Subscribes to the nodes in scope. The listener first receives a {@link Gap} with cause {@code
     * STARTED} per node. Closing the subscription stops delivery.
     */
    AutoCloseable subscribe(Scope scope, NotificationListener listener);

    /** Each node's stream state. */
    Map<NodeId, StreamState> state();
}
