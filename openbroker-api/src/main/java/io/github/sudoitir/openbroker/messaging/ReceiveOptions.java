package io.github.sudoitir.openbroker.messaging;

import io.github.sudoitir.openbroker.Incubating;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * How a receiver reads.
 *
 * @param filter which messages it reads; empty for all
 * @param producedAtOrBefore when present, only messages produced at or before it, so a read of a
 *     subscription that is still being produced to has an end
 * @param transacted settle in batches with {@code commit()} and {@code rollback()}; needs the {@code
 *     transport.transacted} capability
 * @param prefetch how many messages the system may deliver ahead of {@code receive}; zero hands over
 *     the next only once the last is settled, so a slow receiver holds one message and the rest stay
 *     available to others. Empty leaves the driver's bounded default. A system that buffers by size
 *     rather than count treats any positive value as its default
 */
@Incubating
public record ReceiveOptions(
        ReceiveMode mode,
        Optional<MessageFilter> filter,
        Optional<Instant> producedAtOrBefore,
        boolean transacted,
        OptionalInt prefetch) {

    public ReceiveOptions {
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(filter, "filter");
        Objects.requireNonNull(producedAtOrBefore, "producedAtOrBefore");
        Objects.requireNonNull(prefetch, "prefetch");
        if (prefetch.isPresent() && prefetch.getAsInt() < 0) {
            throw new IllegalArgumentException("prefetch must not be negative");
        }
    }

    /** A receiver that takes messages, settling each on its own. */
    public static ReceiveOptions consume() {
        return new ReceiveOptions(ReceiveMode.CONSUME, Optional.empty(), Optional.empty(), false, OptionalInt.empty());
    }

    /** A receiver that reads messages and leaves them where they are. */
    public static ReceiveOptions browse(Optional<MessageFilter> filter) {
        return new ReceiveOptions(ReceiveMode.BROWSE, filter, Optional.empty(), false, OptionalInt.empty());
    }

    public ReceiveOptions asTransacted() {
        return new ReceiveOptions(mode, filter, producedAtOrBefore, true, prefetch);
    }

    public ReceiveOptions withPrefetch(int messages) {
        return new ReceiveOptions(mode, filter, producedAtOrBefore, transacted, OptionalInt.of(messages));
    }

    /** These options, reading only messages produced at or before {@code bound}. */
    public ReceiveOptions producedAtOrBefore(Instant bound) {
        return new ReceiveOptions(mode, filter, Optional.of(bound), transacted, prefetch);
    }
}
