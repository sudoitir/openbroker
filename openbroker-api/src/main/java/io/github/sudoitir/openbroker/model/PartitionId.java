package io.github.sudoitir.openbroker.model;

import java.util.Objects;

/** A partition's identifier, as the system names it ({@code 3}, {@code orders-2}). */
public record PartitionId(String value) {

    public PartitionId {
        Objects.requireNonNull(value, "value");
    }

    public static PartitionId of(String value) {
        return new PartitionId(value);
    }

    @Override
    public String toString() {
        return value;
    }
}
