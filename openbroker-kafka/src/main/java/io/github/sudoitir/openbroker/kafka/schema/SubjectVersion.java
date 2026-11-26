package io.github.sudoitir.openbroker.kafka.schema;

import java.util.Objects;

/**
 * One version of a subject's schema, as the registry keeps it.
 *
 * @param id the registry's schema id, the number a record carries in front of its payload
 * @param version the subject's own version number, which starts at 1 and is contiguous
 * @param type the kind of schema
 * @param schema the schema's own text
 * @param deleted whether this version is soft-deleted: still readable by id, not listed for a new
 *     registration's compatibility check
 */
public record SubjectVersion(int id, int version, SchemaType type, String schema, boolean deleted) {

    public SubjectVersion {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(schema, "schema");
    }
}
