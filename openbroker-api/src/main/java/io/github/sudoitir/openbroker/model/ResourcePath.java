package io.github.sudoitir.openbroker.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The segments that name a resource within its kind, outermost first ({@code [orders]}, or
 * {@code [eu-vhost, orders]} where a namespace contains it). A segment may contain any character.
 */
public record ResourcePath(List<String> segments) {

    public ResourcePath {
        segments = List.copyOf(segments);
        if (segments.isEmpty()) {
            throw new IllegalArgumentException("A path has at least one segment");
        }
        for (String s : segments) {
            if (s.isEmpty()) {
                throw new IllegalArgumentException("A path segment cannot be empty");
            }
        }
    }

    public static ResourcePath of(String... segments) {
        return new ResourcePath(List.of(segments));
    }

    /** The innermost segment: the resource's own name. */
    public String name() {
        return segments.getLast();
    }

    public ResourcePath child(String segment) {
        var next = new ArrayList<>(segments);
        next.add(Objects.requireNonNull(segment, "segment"));
        return new ResourcePath(next);
    }
}
