package io.github.sudoitir.openbroker.messaging;

import io.github.sudoitir.openbroker.Incubating;
import java.util.Objects;
import java.util.Optional;

/**
 * How a receiver reads.
 *
 * @param transacted settle in batches with {@code commit()} and {@code rollback()}; needs the {@code
 *     transport.transacted} capability
 */
@Incubating
public record ReceiveOptions(ReceiveMode mode, Optional<MessageFilter> filter, boolean transacted) {

    public ReceiveOptions {
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(filter, "filter");
    }
}
