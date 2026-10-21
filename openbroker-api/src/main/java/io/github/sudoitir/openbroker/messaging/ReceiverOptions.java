package io.github.sudoitir.openbroker.messaging;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * How a receiver reads.
 *
 * @param mode whether it takes messages or only reads them
 * @param guarantee what the link promises; {@code TRANSACTED} settles in batches with {@code commit()}
 *     and {@code rollback()}
 * @param filter which messages it reads; empty for all
 * @param producedAtOrBefore when present, only messages produced at or before it, so a read of a
 *     subscription that is still being produced to has an end
 * @param flow how much it takes in
 * @param recovery how the link reopens after a loss
 */
public record ReceiverOptions(
        ReceiveMode mode,
        Guarantee guarantee,
        Optional<MessageFilter> filter,
        Optional<Instant> producedAtOrBefore,
        FlowControl flow,
        Recovery recovery) {

    public ReceiverOptions {
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(guarantee, "guarantee");
        Objects.requireNonNull(filter, "filter");
        Objects.requireNonNull(producedAtOrBefore, "producedAtOrBefore");
        Objects.requireNonNull(flow, "flow");
        Objects.requireNonNull(recovery, "recovery");
    }

    /** A receiver that takes messages, settling each on its own, at least once. */
    public static ReceiverOptions consume() {
        return new ReceiverOptions(
                ReceiveMode.CONSUME,
                Guarantee.AT_LEAST_ONCE,
                Optional.empty(),
                Optional.empty(),
                FlowControl.defaults(),
                Recovery.defaults());
    }

    /** A receiver that reads messages and leaves them where they are. */
    public static ReceiverOptions browse(Optional<MessageFilter> filter) {
        return new ReceiverOptions(
                ReceiveMode.BROWSE,
                Guarantee.AT_LEAST_ONCE,
                filter,
                Optional.empty(),
                FlowControl.defaults(),
                Recovery.defaults());
    }

    public ReceiverOptions withGuarantee(Guarantee guarantee) {
        return new ReceiverOptions(mode, guarantee, filter, producedAtOrBefore, flow, recovery);
    }

    public ReceiverOptions withFlow(FlowControl flow) {
        return new ReceiverOptions(mode, guarantee, filter, producedAtOrBefore, flow, recovery);
    }

    public ReceiverOptions withRecovery(Recovery recovery) {
        return new ReceiverOptions(mode, guarantee, filter, producedAtOrBefore, flow, recovery);
    }

    /** These options, reading only messages produced at or before {@code bound}. */
    public ReceiverOptions producedAtOrBefore(Instant bound) {
        return new ReceiverOptions(mode, guarantee, filter, Optional.of(bound), flow, recovery);
    }
}
