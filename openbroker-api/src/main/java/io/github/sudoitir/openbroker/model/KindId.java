package io.github.sudoitir.openbroker.model;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A driver-defined resource kind: the system id, a dot, and the kind's own name, in lower case
 * ({@code artemis.queue}, {@code kafka.group-topic}). A kind maps to exactly one {@link ResourceClass}.
 */
public record KindId(String value) implements Comparable<KindId> {

    private static final Pattern SHAPE = Pattern.compile("[a-z][a-z0-9]*(\\.[a-z0-9][a-z0-9-]*)+");

    public KindId {
        Objects.requireNonNull(value, "value");
        if (!SHAPE.matcher(value).matches()) {
            throw new IllegalArgumentException("A kind is <system>.<name> in lower case: " + value);
        }
    }

    public static KindId of(String value) {
        return new KindId(value);
    }

    /** The system that defines this kind. */
    public String system() {
        return value.substring(0, value.indexOf('.'));
    }

    @Override
    public int compareTo(KindId other) {
        return value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return value;
    }
}
