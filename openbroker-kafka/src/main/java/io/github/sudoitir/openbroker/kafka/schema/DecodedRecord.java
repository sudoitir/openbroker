package io.github.sudoitir.openbroker.kafka.schema;

/**
 * What a payload carrying a schema id decoded to, or why it did not.
 *
 * <p>There are exactly two outcomes. An {@link Undecoded} payload is never presented as an empty
 * record: a tombstone is {@link #NO_SCHEMA_ID}, not {@code {}}, and a registry that does not hold the
 * id is {@link #SCHEMA_NOT_FOUND}. A record that is genuinely empty is only what a schema itself
 * decoded to an empty value.
 */
public sealed interface DecodedRecord {

    /** The payload carried no schema framing at all; a tombstone is this, not an empty body. */
    String NO_SCHEMA_ID = "no-schema-id";

    /** The payload names a schema id the registry does not hold. */
    String SCHEMA_NOT_FOUND = "schema-not-found";

    /** A serde was named that this installation does not run. */
    String SERDE_NOT_REGISTERED = "serde-not-registered";

    /** A serde ran and refused the payload. */
    String SERDE_REFUSED = "serde-refused";

    /** The connection has no registry configured, so nothing decodes. */
    String NO_REGISTRY = "no-registry";

    /** The body was cut short of its schema, so it is not decoded rather than decoded wrongly. */
    String TRUNCATED = "truncated";

    /**
     * A decoded record.
     *
     * @param type the kind of schema that decoded it
     * @param schemaId the registry's schema id the payload carried
     * @param subject the subject the id resolved to, when the registry answered with one
     * @param record the record as maps, lists and scalars, so a caller serialises it without a second
     *     pass over a string
     */
    record Decoded(SchemaType type, int schemaId, String subject, Object record) implements DecodedRecord {

        public Decoded {
            if (type == null) {
                throw new IllegalArgumentException("type");
            }
            if (subject == null) {
                throw new IllegalArgumentException("subject");
            }
            if (record == null) {
                throw new IllegalArgumentException("record");
            }
        }
    }

    /**
     * A payload that was not decoded.
     *
     * @param reason one of this interface's constants, so a caller switches on it rather than on
     *     prose
     */
    record Undecoded(String reason) implements DecodedRecord {

        public Undecoded {
            if (reason == null || reason.isBlank()) {
                throw new IllegalArgumentException("reason");
            }
        }
    }
}
