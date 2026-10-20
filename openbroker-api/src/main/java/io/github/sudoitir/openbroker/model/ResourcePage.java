package io.github.sudoitir.openbroker.model;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** One page of a list, and the token for the next one when there is more. */
public record ResourcePage(List<Resource> items, Optional<String> continueToken) {

    public ResourcePage {
        items = List.copyOf(items);
        Objects.requireNonNull(continueToken, "continueToken");
    }
}
