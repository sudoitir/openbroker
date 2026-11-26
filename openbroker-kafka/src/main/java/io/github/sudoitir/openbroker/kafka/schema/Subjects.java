package io.github.sudoitir.openbroker.kafka.schema;

import io.github.sudoitir.openbroker.messaging.Cursor;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A page of subjects, in name order.
 *
 * @param items the subjects on this page
 * @param next the cursor for the next page, or empty on the last one
 */
public record Subjects(List<Subject> items, Optional<Cursor> next) {

    public Subjects {
        items = List.copyOf(items);
        Objects.requireNonNull(next, "next");
    }
}
