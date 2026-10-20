package io.github.sudoitir.openbroker;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * One connection option a driver accepts. A sensitive option's value is never logged, shown in an
 * exception or returned in error metadata.
 */
public record OptionSpec(
        String key,
        Type type,
        boolean required,
        boolean sensitive,
        Optional<String> defaultValue,
        List<String> allowedValues,
        String description,
        Optional<Plane> plane) {

    public enum Type {
        STRING,
        INTEGER,
        BOOLEAN,
        DURATION,
        URI
    }

    public OptionSpec {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(defaultValue, "defaultValue");
        allowedValues = List.copyOf(allowedValues);
        Objects.requireNonNull(description, "description");
        Objects.requireNonNull(plane, "plane");
    }
}
