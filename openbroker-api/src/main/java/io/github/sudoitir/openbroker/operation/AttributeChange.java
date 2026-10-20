package io.github.sudoitir.openbroker.operation;

import java.util.Objects;
import java.util.Optional;

/** One attribute an operation would change. */
public record AttributeChange(String key, Optional<Object> before, Optional<Object> after) {

    public AttributeChange {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(before, "before");
        Objects.requireNonNull(after, "after");
    }
}
