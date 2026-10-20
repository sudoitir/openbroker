package io.github.sudoitir.openbroker.messaging;

import io.github.sudoitir.openbroker.Incubating;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * How a received message is settled: the AMQP 1.0 outcomes, which every system's acknowledgement
 * maps onto.
 */
@Incubating
public sealed interface Disposition {

    /** Processed: remove it. */
    record Accept() implements Disposition {}

    /**
     * Not processed: make it available again, after a delay where the system supports one. With
     * {@code deliveryFailed} the delivery counts as a failed attempt.
     */
    record Release(Optional<Duration> delay, boolean deliveryFailed) implements Disposition {
        public Release {
            Objects.requireNonNull(delay, "delay");
        }
    }

    /** Cannot be processed: dead-letter or drop it, as the system's policy says. */
    record Reject(Optional<String> reason) implements Disposition {
        public Reject {
            Objects.requireNonNull(reason, "reason");
        }
    }

    static Disposition accept() {
        return new Accept();
    }
}
