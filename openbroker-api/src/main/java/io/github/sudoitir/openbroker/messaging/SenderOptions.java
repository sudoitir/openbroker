package io.github.sudoitir.openbroker.messaging;

import java.util.Objects;

/**
 * How a sender sends.
 *
 * @param guarantee what the link promises
 * @param idempotent the system deduplicates the sender's own resends by message id
 * @param ordering which messages stay in order across a resend; any but {@code NONE} with {@code
 *     AT_LEAST_ONCE} needs {@code idempotent}
 * @param buffer the bound on unconfirmed messages
 * @param recovery how the link reopens after a loss
 */
public record SenderOptions(
        Guarantee guarantee, boolean idempotent, OrderingScope ordering, SendBuffer buffer, Recovery recovery) {

    public SenderOptions {
        Objects.requireNonNull(guarantee, "guarantee");
        Objects.requireNonNull(ordering, "ordering");
        Objects.requireNonNull(buffer, "buffer");
        Objects.requireNonNull(recovery, "recovery");
    }

    /** At least once, not idempotent, unordered, with the default buffer and recovery. */
    public static SenderOptions defaults() {
        return new SenderOptions(
                Guarantee.AT_LEAST_ONCE, false, OrderingScope.NONE, SendBuffer.defaults(), Recovery.defaults());
    }

    public SenderOptions withGuarantee(Guarantee guarantee) {
        return new SenderOptions(guarantee, idempotent, ordering, buffer, recovery);
    }

    public SenderOptions withIdempotent(boolean idempotent) {
        return new SenderOptions(guarantee, idempotent, ordering, buffer, recovery);
    }

    public SenderOptions withOrdering(OrderingScope ordering) {
        return new SenderOptions(guarantee, idempotent, ordering, buffer, recovery);
    }

    public SenderOptions withBuffer(SendBuffer buffer) {
        return new SenderOptions(guarantee, idempotent, ordering, buffer, recovery);
    }

    public SenderOptions withRecovery(Recovery recovery) {
        return new SenderOptions(guarantee, idempotent, ordering, buffer, recovery);
    }
}
