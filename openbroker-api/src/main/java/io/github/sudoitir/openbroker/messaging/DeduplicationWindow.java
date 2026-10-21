package io.github.sudoitir.openbroker.messaging;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * How far back a system remembers message ids to drop repeats.
 *
 * @param ids how many ids it remembers, where it bounds by count
 * @param time how long it remembers an id, where it bounds by time
 * @param durable whether the memory survives a restart of the system
 */
public record DeduplicationWindow(OptionalLong ids, Optional<Duration> time, boolean durable) {

    public DeduplicationWindow {
        Objects.requireNonNull(ids, "ids");
        Objects.requireNonNull(time, "time");
        if (ids.isPresent() && ids.getAsLong() <= 0) {
            throw new IllegalArgumentException("ids must be positive");
        }
        if (time.isPresent() && !time.get().isPositive()) {
            throw new IllegalArgumentException("time must be positive");
        }
    }
}
