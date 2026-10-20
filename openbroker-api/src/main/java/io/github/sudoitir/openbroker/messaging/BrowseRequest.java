package io.github.sudoitir.openbroker.messaging;

import java.util.Objects;
import java.util.Optional;

/**
 * A browse: from where, how many, which messages, and the most body bytes to return per message (a
 * longer body is cut and marked truncated).
 */
public record BrowseRequest(Position position, int limit, Optional<MessageFilter> filter, long maxBodyBytes) {

    public BrowseRequest {
        Objects.requireNonNull(position, "position");
        Objects.requireNonNull(filter, "filter");
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        if (maxBodyBytes < 0) {
            throw new IllegalArgumentException("maxBodyBytes must not be negative");
        }
    }
}
