package io.github.sudoitir.openbroker.messaging;

import java.util.Objects;

/** How many messages there are, what is counted, and how far to trust it. */
public record Total(Denominator of, Count count) {

    public Total {
        Objects.requireNonNull(of, "of");
        Objects.requireNonNull(count, "count");
    }
}
