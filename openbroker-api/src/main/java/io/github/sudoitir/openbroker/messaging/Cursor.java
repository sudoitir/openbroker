package io.github.sudoitir.openbroker.messaging;

import java.util.Objects;

/**
 * An opaque continuation of a browse. Valid across reconnects of one connection; after the
 * subscription was recreated it fails with {@code FAILED_PRECONDITION}, and past retention with
 * {@code OUT_OF_RANGE}.
 */
public record Cursor(String token) {

    public Cursor {
        Objects.requireNonNull(token, "token");
    }
}
