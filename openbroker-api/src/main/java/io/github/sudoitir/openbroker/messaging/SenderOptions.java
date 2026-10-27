package io.github.sudoitir.openbroker.messaging;

import java.util.Objects;

/**
 * How a sender sends.
 *
 * @param guarantee what the link promises
 * @param deduplication the minimum scope in which the system drops the sender's repeated messages; a
 *     transport that offers less refuses the sender with {@code UNIMPLEMENTED}
 * @param ordering which messages stay in order across a resend; any but {@code NONE} with {@code
 *     AT_LEAST_ONCE} needs at least {@code SESSION}
 * @param buffer the bound on unconfirmed messages
 * @param recovery how the link reopens after a loss
 */
public record SenderOptions(
        Guarantee guarantee,
        DeduplicationScope deduplication,
        OrderingScope ordering,
        SendBuffer buffer,
        Recovery recovery) {

    public SenderOptions {
        Objects.requireNonNull(guarantee, "guarantee");
        Objects.requireNonNull(deduplication, "deduplication");
        Objects.requireNonNull(ordering, "ordering");
        Objects.requireNonNull(buffer, "buffer");
        Objects.requireNonNull(recovery, "recovery");
    }

    /** At least once, no deduplication, unordered, with the default buffer and recovery. */
    public static SenderOptions defaults() {
        return new SenderOptions(
                Guarantee.AT_LEAST_ONCE,
                DeduplicationScope.NONE,
                OrderingScope.NONE,
                SendBuffer.defaults(),
                Recovery.defaults());
    }

    public SenderOptions withGuarantee(Guarantee guarantee) {
        return new SenderOptions(guarantee, deduplication, ordering, buffer, recovery);
    }

    public SenderOptions withDeduplication(DeduplicationScope deduplication) {
        return new SenderOptions(guarantee, deduplication, ordering, buffer, recovery);
    }

    public SenderOptions withOrdering(OrderingScope ordering) {
        return new SenderOptions(guarantee, deduplication, ordering, buffer, recovery);
    }

    public SenderOptions withBuffer(SendBuffer buffer) {
        return new SenderOptions(guarantee, deduplication, ordering, buffer, recovery);
    }

    public SenderOptions withRecovery(Recovery recovery) {
        return new SenderOptions(guarantee, deduplication, ordering, buffer, recovery);
    }
}
