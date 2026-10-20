package io.github.sudoitir.openbroker.messaging;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** One page of a browse: the messages, a cursor to continue, and the total. */
public record BrowsePage(List<Message> messages, Optional<Cursor> next, Total total) {

    public BrowsePage {
        messages = List.copyOf(messages);
        Objects.requireNonNull(next, "next");
        Objects.requireNonNull(total, "total");
    }
}
