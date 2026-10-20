package io.github.sudoitir.openbroker.notification;

import io.github.sudoitir.openbroker.BrokerException;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/** The state of one node's notification stream. */
public record StreamState(Phase phase, Optional<Instant> since, Optional<BrokerException> lastFailure) {

    public enum Phase {
        CONNECTING,
        ACTIVE,
        /** Waiting before the next reconnect attempt. */
        BACKING_OFF,
        /** The system refuses the subscription, for instance for the account's permissions. */
        UNAVAILABLE
    }

    public StreamState {
        Objects.requireNonNull(phase, "phase");
        Objects.requireNonNull(since, "since");
        Objects.requireNonNull(lastFailure, "lastFailure");
    }
}
