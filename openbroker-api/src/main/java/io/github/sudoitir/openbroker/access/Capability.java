package io.github.sudoitir.openbroker.access;

import java.util.Objects;

/**
 * Something an account may or may not be able to do on a cluster. Open: standard capabilities are
 * constants here, and a driver adds its own under its system's namespace.
 */
public record Capability(String id) {

    /** Reading resources and metrics. */
    public static final Capability MANAGEMENT_READ = new Capability("management.read");
    /** Creating, changing and deleting resources. */
    public static final Capability MANAGEMENT_WRITE = new Capability("management.write");
    /** Subscribing to notifications. */
    public static final Capability NOTIFICATIONS = new Capability("notifications");
    /** Browsing messages. */
    public static final Capability BROWSE = new Capability("messages.browse");
    /** Sending messages. */
    public static final Capability SEND = new Capability("messages.send");
    /** Receiving messages through the transport. */
    public static final Capability RECEIVE = new Capability("messages.receive");
    /** Transacted batches in the transport. */
    public static final Capability TRANSPORT_TRANSACTED = new Capability("transport.transacted");

    public Capability {
        Objects.requireNonNull(id, "id");
    }

    @Override
    public String toString() {
        return id;
    }
}
