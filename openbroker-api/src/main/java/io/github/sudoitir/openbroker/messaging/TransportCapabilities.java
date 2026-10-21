package io.github.sudoitir.openbroker.messaging;

import java.util.Objects;
import java.util.Set;

/**
 * What a transport can do on a system. Static: opening a link outside it fails with {@code
 * UNIMPLEMENTED}.
 *
 * @param guarantees the guarantees a link can have
 * @param deduplication whether the system drops repeated message ids
 * @param orderings the orderings a link can keep
 * @param prefetchBounds the units in which the system bounds what it delivers ahead
 * @param pullSettlement what a release on a pull receiver covers
 * @param joinedTransactions whether a sender can join a receiver's transaction
 * @param retry who enforces each part of a retry policy
 * @param failover whether a link follows a failover to another node
 */
public record TransportCapabilities(
        Set<Guarantee> guarantees,
        boolean deduplication,
        Set<OrderingScope> orderings,
        Set<PrefetchBound> prefetchBounds,
        SettlementScope pullSettlement,
        boolean joinedTransactions,
        RetrySupport retry,
        boolean failover) {

    public TransportCapabilities {
        guarantees = Set.copyOf(guarantees);
        orderings = Set.copyOf(orderings);
        prefetchBounds = Set.copyOf(prefetchBounds);
        Objects.requireNonNull(pullSettlement, "pullSettlement");
        Objects.requireNonNull(retry, "retry");
    }
}
