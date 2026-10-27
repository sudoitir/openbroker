package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.model.ResourceQuery;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/** A query as the handlers read it: the name glob, the field selector and the projection. */
record Listing(ResourceQuery query) {

    /** The glob over the resource's name ({@code *} and {@code ?}), as a predicate. */
    Predicate<String> names() {
        Optional<String> glob = query.namePattern();
        if (glob.isEmpty()) {
            return name -> true;
        }
        StringBuilder regex = new StringBuilder();
        for (char c : glob.get().toCharArray()) {
            switch (c) {
                case '*' -> regex.append(".*");
                case '?' -> regex.append('.');
                default -> regex.append(Pattern.quote(String.valueOf(c)));
            }
        }
        Pattern pattern = Pattern.compile(regex.toString());
        return name -> pattern.matcher(name).matches();
    }

    Optional<String> field(String key) {
        return Optional.ofNullable(query.fieldSelector().get(key));
    }

    /** Whether the caller wants an attribute: it asked for all of them, or named this one. */
    boolean wants(String attribute) {
        Set<String> projection = query.projection();
        return projection.isEmpty() || projection.contains(attribute);
    }

    /** Whether the caller wants an attribute with the prefix, such as every topic setting. */
    boolean wantsPrefix(String prefix) {
        Set<String> projection = query.projection();
        return projection.isEmpty() || projection.stream().anyMatch(k -> k.startsWith(prefix));
    }
}
