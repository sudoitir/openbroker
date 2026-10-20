package io.github.sudoitir.openbroker.messaging;

import io.github.sudoitir.openbroker.model.PartitionId;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.util.Objects;
import java.util.Optional;

/** Where a message is sent. */
public sealed interface SendTarget {

    /** To a destination, routed by the system, optionally with a routing key or to a partition. */
    record ToDestination(ResourceId destination, Optional<String> routingKey, Optional<PartitionId> partition)
            implements SendTarget {
        public ToDestination {
            Objects.requireNonNull(destination, "destination");
            Objects.requireNonNull(routingKey, "routingKey");
            Objects.requireNonNull(partition, "partition");
        }
    }

    /** Straight to one subscription, bypassing routing: the only way to reach a standalone queue. */
    record ToSubscription(ResourceId subscription) implements SendTarget {
        public ToSubscription {
            Objects.requireNonNull(subscription, "subscription");
        }
    }

    static SendTarget destination(ResourceId destination) {
        return new ToDestination(destination, Optional.empty(), Optional.empty());
    }

    static SendTarget subscription(ResourceId subscription) {
        return new ToSubscription(subscription);
    }
}
