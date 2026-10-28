package io.github.sudoitir.openbroker.messaging;

import java.util.Objects;
import java.util.Set;

/**
 * What a transport can do on one kind of subscription. Kinds of one system differ: a group that shares
 * messages between its members orders, settles and retries differently from one that gives each member
 * its own partitions.
 *
 * @param orderings the orderings a processor on this kind can keep
 * @param pullSettlement what a release on a pull receiver covers
 * @param joinedTransactions whether a sender can join a processor's transaction
 * @param retry what the system enforces under {@code RetryMode.SYSTEM}
 * @param releaseCountsDelivery whether a release that is not a failed delivery ({@code
 *     Disposition.Release} with {@code deliveryFailed} false) still counts as a delivery attempt, so that
 *     the message moves towards the system's delivery limit; true where the system counts every
 *     redelivery itself, false where only a failed delivery counts
 */
public record SubscriptionCapabilities(
        Set<OrderingScope> orderings,
        SettlementScope pullSettlement,
        boolean joinedTransactions,
        SystemRetry retry,
        boolean releaseCountsDelivery) {

    public SubscriptionCapabilities {
        orderings = Set.copyOf(orderings);
        Objects.requireNonNull(pullSettlement, "pullSettlement");
        Objects.requireNonNull(retry, "retry");
    }
}
