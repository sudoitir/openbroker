package io.github.sudoitir.openbroker.model;

import java.util.List;
import java.util.Optional;

/** The attributes a kind has. */
public record AttributeSchema(List<AttributeSpec> attributes) {

    public AttributeSchema {
        attributes = List.copyOf(attributes);
    }

    public Optional<AttributeSpec> attribute(String key) {
        return attributes.stream().filter(a -> a.key().equals(key)).findFirst();
    }
}
