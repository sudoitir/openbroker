package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.messaging.LinkListener;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import java.util.List;

/** The observers that watch one connection: the starter wires them from Micrometer and OpenTelemetry. */
interface ObserverSource {

    /** The observer for the connection's operations, tagged with its name and system. */
    MessagingObserver observer(String connection, String system);

    /** The listeners of the connection's link events, which the transport is asked to call. */
    List<LinkListener> linkListeners(String connection, String system);
}
