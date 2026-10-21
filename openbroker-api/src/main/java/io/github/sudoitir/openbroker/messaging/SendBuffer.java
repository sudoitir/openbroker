package io.github.sudoitir.openbroker.messaging;

import java.time.Duration;
import java.util.Objects;

/**
 * The bound on messages a sender has accepted and the system has not yet confirmed.
 *
 * @param maxInFlight the most messages
 * @param maxBytes the most body bytes
 * @param maxBlock how long {@code send} waits for room before failing with {@code SEND_BUFFER_FULL}
 */
public record SendBuffer(int maxInFlight, long maxBytes, Duration maxBlock) {

    public SendBuffer {
        Objects.requireNonNull(maxBlock, "maxBlock");
        if (maxInFlight <= 0) {
            throw new IllegalArgumentException("maxInFlight must be positive");
        }
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be positive");
        }
        if (!maxBlock.isPositive()) {
            throw new IllegalArgumentException("maxBlock must be positive");
        }
    }

    /** 1000 messages, 16 MiB, 30 s. */
    public static SendBuffer defaults() {
        return new SendBuffer(1000, 16L * 1024 * 1024, Duration.ofSeconds(30));
    }
}
