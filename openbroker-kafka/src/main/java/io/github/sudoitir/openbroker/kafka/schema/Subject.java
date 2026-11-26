package io.github.sudoitir.openbroker.kafka.schema;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * One subject: the name a registry files a topic's schemas under, usually {@code <topic>-key} or
 * {@code <topic>-value}.
 *
 * @param name the subject's name
 * @param versions every version the registry lists, oldest first, deleted ones included when the
 *     registry shows them
 * @param compatibility the level this subject is held at, or empty when the registry resolves it from
 *     the registry default and does not restate it here
 * @param compatibilityNote the registry's own words when its level is one this driver does not name, kept
 *     beside {@link CompatibilityLevel#NONE}, or a sentence about why the level is not known when
 *     {@code compatibility} is empty. Empty when the level is one this driver names.
 */
public record Subject(
        String name,
        List<SubjectVersion> versions,
        Optional<CompatibilityLevel> compatibility,
        Optional<String> compatibilityNote) {

    public Subject {
        Objects.requireNonNull(name, "name");
        versions = List.copyOf(versions);
        Objects.requireNonNull(compatibility, "compatibility");
        Objects.requireNonNull(compatibilityNote, "compatibilityNote");
    }
}
