package io.github.sudoitir.openbroker.kafka.schema;

/**
 * How a registry decides whether a new schema may join a subject. The levels are the ones a registry
 * that carries all of {@code BACKWARD}, {@code FORWARD} and {@code FULL} understands; {@link #NONE}
 * turns the check off.
 *
 * <p>A level this driver does not name reads as {@link #NONE}, and the registry's own words are kept
 * in {@link Subject#compatibilityNote()} rather than being lost.
 */
public enum CompatibilityLevel {
    BACKWARD,
    BACKWARD_TRANSITIVE,
    FORWARD,
    FORWARD_TRANSITIVE,
    FULL,
    FULL_TRANSITIVE,
    NONE
}
