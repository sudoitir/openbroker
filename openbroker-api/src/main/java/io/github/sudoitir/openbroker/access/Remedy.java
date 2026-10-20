package io.github.sudoitir.openbroker.access;

import java.util.Objects;

/** How the broker can be changed to allow a capability: what to do, and a snippet in its own format. */
public record Remedy(String text, String snippet, String mediaType) {

    public Remedy {
        Objects.requireNonNull(text, "text");
        Objects.requireNonNull(snippet, "snippet");
        Objects.requireNonNull(mediaType, "mediaType");
    }
}
