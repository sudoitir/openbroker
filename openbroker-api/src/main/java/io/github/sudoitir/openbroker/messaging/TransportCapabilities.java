package io.github.sudoitir.openbroker.messaging;

import io.github.sudoitir.openbroker.model.KindId;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * What a transport can do on a system. Static: opening a link outside it fails with {@code
 * UNIMPLEMENTED}.
 *
 * @param guarantees the guarantees a link can have
 * @param deduplication the strongest scope in which the system drops repeated messages
 * @param orderings the orderings a sender can keep
 * @param prefetchBounds the units in which the system bounds what it delivers ahead
 * @param subscriptions what the transport can do on each kind of subscription it offers
 * @param failover whether a link follows a failover to another node
 */
public record TransportCapabilities(
        Set<Guarantee> guarantees,
        DeduplicationScope deduplication,
        Set<OrderingScope> orderings,
        Set<PrefetchBound> prefetchBounds,
        Map<KindId, SubscriptionCapabilities> subscriptions,
        boolean failover) {

    public TransportCapabilities {
        guarantees = Set.copyOf(guarantees);
        Objects.requireNonNull(deduplication, "deduplication");
        orderings = Set.copyOf(orderings);
        prefetchBounds = Set.copyOf(prefetchBounds);
        subscriptions = Map.copyOf(subscriptions);
    }

    /**
     * What the transport can do on {@code kind}.
     *
     * @throws IllegalArgumentException when the transport offers no subscription of that kind
     */
    public SubscriptionCapabilities subscription(KindId kind) {
        SubscriptionCapabilities capabilities = subscriptions.get(kind);
        if (capabilities == null) {
            throw new IllegalArgumentException("The transport offers no subscription of kind " + kind);
        }
        return capabilities;
    }
}
