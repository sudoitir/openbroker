package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.artemis.ArtemisKinds;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.util.Optional;

/** Where a send goes, in Core terms. */
final class Targets {

    private static final String QUEUE_SEPARATOR = "::";

    private Targets() {}

    /**
     * A send target on a node.
     *
     * @param resource what the target names
     * @param subscription the queue when the target is one
     * @param destination the Core address, or the queue's fully qualified name
     * @param address the Core address either way
     */
    record Resolved(ResourceId resource, Optional<ResourceId> subscription, String destination, String address) {}

    static Resolved resolve(Cluster cluster, NodeHandle node, SendTarget target, Deadline deadline) {
        return switch (target) {
            case SendTarget.ToSubscription s -> {
                requireQueue(s.subscription());
                String destination = Destinations.of(cluster, node, s.subscription(), deadline);
                yield new Resolved(
                        s.subscription(), Optional.of(s.subscription()), destination, addressOf(destination));
            }
            case SendTarget.ToDestination d -> {
                if (!d.destination().kind().equals(ArtemisKinds.ADDRESS)
                        || d.partition().isPresent()
                        || d.routingKey().isPresent()) {
                    throw Queries.invalid("An Artemis destination is an address, with no partition or key.");
                }
                yield new Resolved(
                        d.destination(),
                        Optional.empty(),
                        d.destination().name(),
                        d.destination().name());
            }
        };
    }

    static void requireQueue(ResourceId id) {
        if (!id.kind().equals(ArtemisKinds.QUEUE)) {
            throw Queries.invalid(
                    "Artemis holds messages in a queue, " + ArtemisKinds.QUEUE + "; not " + id.kind() + ".");
        }
    }

    /** The address part of a fully qualified queue name. */
    static String addressOf(String destination) {
        int at = destination.indexOf(QUEUE_SEPARATOR);
        return at < 0 ? destination : destination.substring(0, at);
    }
}
