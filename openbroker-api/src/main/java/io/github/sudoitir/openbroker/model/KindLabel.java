package io.github.sudoitir.openbroker.model;

import java.util.Objects;

/**
 * How a person names a kind: its singular and plural, in sentence case ("Group on topic", "Groups on
 * topics"). A consumer shows these instead of deriving a name from the kind id.
 */
public record KindLabel(String singular, String plural) {

    public KindLabel {
        Objects.requireNonNull(singular, "singular");
        Objects.requireNonNull(plural, "plural");
        if (singular.isBlank() || plural.isBlank()) {
            throw new IllegalArgumentException("A kind label has a non-blank singular and plural.");
        }
    }

    public static KindLabel of(String singular, String plural) {
        return new KindLabel(singular, plural);
    }
}
