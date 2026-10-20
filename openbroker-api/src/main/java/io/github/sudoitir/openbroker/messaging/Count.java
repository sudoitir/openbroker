package io.github.sudoitir.openbroker.messaging;

import java.util.Objects;

/** A number that says how much it can be trusted. */
public sealed interface Count {

    record Exact(long value) implements Count {}

    /** Derived, such as a backlog computed from offset distances. Never shown as exact. */
    record Estimate(long value) implements Count {}

    /** The system cannot say, and why. An unavailable count is never zero. */
    record Unavailable(String reason) implements Count {
        public Unavailable {
            Objects.requireNonNull(reason, "reason");
        }
    }

    static Count exact(long value) {
        return new Exact(value);
    }

    static Count estimate(long value) {
        return new Estimate(value);
    }

    static Count unavailable(String reason) {
        return new Unavailable(reason);
    }
}
