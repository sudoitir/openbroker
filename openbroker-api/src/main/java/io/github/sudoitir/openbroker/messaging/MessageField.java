package io.github.sudoitir.openbroker.messaging;

import java.util.Objects;

/** A header or property to group messages by, as when triaging a dead-letter queue. */
public record MessageField(Source source, String name) {

    public enum Source {
        HEADER,
        PROPERTY
    }

    public MessageField {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(name, "name");
    }

    public static MessageField header(String name) {
        return new MessageField(Source.HEADER, name);
    }

    public static MessageField property(String name) {
        return new MessageField(Source.PROPERTY, name);
    }
}
