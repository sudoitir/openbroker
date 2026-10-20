package io.github.sudoitir.openbroker;

import java.time.Instant;
import java.util.Optional;

/**
 * The admission of one call. The driver calls {@link #complete} exactly once when the call has an
 * answer or has failed, with the broker's own time when the answer carried one, then {@link
 * #close()}. A permit closed without completing means the call was never made.
 */
public interface Permit extends AutoCloseable {

    void complete(Outcome outcome, Optional<Instant> serverTime);

    @Override
    default void close() {}

    /** A permit that records nothing. */
    static Permit unlimited() {
        return (outcome, serverTime) -> {};
    }
}
