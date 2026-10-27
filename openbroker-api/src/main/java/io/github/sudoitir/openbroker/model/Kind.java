package io.github.sudoitir.openbroker.model;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A kind of resource a driver offers: its label, its class, the verbs it supports, its parent kind, its
 * attributes, and the fields a list of it may be ordered by.
 */
public record Kind(
        KindId id,
        KindLabel label,
        ResourceClass resourceClass,
        Set<Verb> verbs,
        Optional<KindId> parent,
        AttributeSchema schema,
        Set<String> orderings) {

    public Kind {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(label, "label");
        Objects.requireNonNull(resourceClass, "resourceClass");
        verbs = Set.copyOf(verbs);
        Objects.requireNonNull(parent, "parent");
        Objects.requireNonNull(schema, "schema");
        orderings = Set.copyOf(orderings);
    }

    public boolean supports(Verb verb) {
        return verbs.contains(verb);
    }
}
