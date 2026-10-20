package io.github.sudoitir.openbroker.messaging;

import io.github.sudoitir.openbroker.Incubating;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * How a receiver reads.
 *
 * @param transacted settle in batches with {@code commit()} and {@code rollback()}; needs the {@code
 *     transport.transacted} capability
 * @param prefetch how many messages the system may deliver ahead of {@code receive}; zero hands over
 *     the next only once the last is settled, so a slow receiver holds one message and the rest stay
 *     available to others. Empty leaves the driver's bounded default. A system that buffers by size
 *     rather than count treats any positive value as its default
 */
@Incubating
public record ReceiveOptions(
        ReceiveMode mode, Optional<MessageFilter> filter, boolean transacted, OptionalInt prefetch) {

    public ReceiveOptions {
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(filter, "filter");
        Objects.requireNonNull(prefetch, "prefetch");
        if (prefetch.isPresent() && prefetch.getAsInt() < 0) {
            throw new IllegalArgumentException("prefetch must not be negative");
        }
    }

    /** A receiver that takes messages, settling each on its own. */
    public static ReceiveOptions consume() {
        return new ReceiveOptions(ReceiveMode.CONSUME, Optional.empty(), false, OptionalInt.empty());
    }

    /** A receiver that reads messages and leaves them where they are. */
    public static ReceiveOptions browse(Optional<MessageFilter> filter) {
        return new ReceiveOptions(ReceiveMode.BROWSE, filter, false, OptionalInt.empty());
    }

    public ReceiveOptions asTransacted() {
        return new ReceiveOptions(mode, filter, true, prefetch);
    }

    public ReceiveOptions withPrefetch(int messages) {
        return new ReceiveOptions(mode, filter, transacted, OptionalInt.of(messages));
    }
}
