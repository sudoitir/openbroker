package io.github.sudoitir.openbroker.model;

import java.util.Objects;

/** One sort key of a list, in the AIP-132 style. */
public record Ordering(String field, boolean descending) {

    public Ordering {
        Objects.requireNonNull(field, "field");
    }

    public static Ordering asc(String field) {
        return new Ordering(field, false);
    }

    public static Ordering desc(String field) {
        return new Ordering(field, true);
    }
}
