package io.github.sudoitir.openbroker;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Objects;

/**
 * A secret value that never shows itself by accident: {@link #toString()} is redacted, so a secret
 * in a log line, an exception or a debugger summary reads {@code Secret[redacted]}. Only {@link
 * #reveal()} returns it, at the one place that must send it.
 */
public final class Secret {

    private static final Secret EMPTY = new Secret("");

    private final String value;

    private Secret(String value) {
        this.value = Objects.requireNonNull(value, "value");
    }

    public static Secret of(String value) {
        return value.isEmpty() ? EMPTY : new Secret(value);
    }

    public static Secret empty() {
        return EMPTY;
    }

    public String reveal() {
        return value;
    }

    public boolean isEmpty() {
        return value.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Secret other
                && MessageDigest.isEqual(
                        value.getBytes(StandardCharsets.UTF_8), other.value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public String toString() {
        return "Secret[redacted]";
    }
}
