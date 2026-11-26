package io.github.sudoitir.openbroker.kafka.schema;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.access.Capability;
import io.github.sudoitir.openbroker.messaging.Cursor;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.operation.Precondition;
import io.github.sudoitir.openbroker.operation.Preview;
import java.util.Map;

/**
 * The schema registry one Kafka cluster is configured with, reached through
 * {@code ClusterConnection.unwrap(SchemaCatalog.class)} because a subject is not a resource every
 * driver has.
 *
 * <p>Every change is previewed first: a preview writes nothing and carries an etag over the subject's
 * state, and passing that etag to the corresponding change through {@link Precondition#ifMatch} makes
 * the change run only on what was previewed. A stale etag is refused with
 * {@code FAILED_PRECONDITION}/{@code PRECONDITION_FAILED} and writes nothing.
 *
 * <p>A registry that does not offer an operation says so through {@link #capability()} and refuses
 * that method with a {@code BrokerException} naming the registry's reason; it never fakes the
 * operation. A connection with no registry configured reports the capability unavailable with that
 * reason, and nothing here invents a subject.
 */
public interface SchemaCatalog extends AutoCloseable {

    /** The capability that says whether this cluster's registry is configured and reachable. */
    Capability SCHEMA_REGISTRY = new Capability("kafka.schema-registry");

    /** Whether a registry is configured, why not, and what would make it so. Unknown is not unavailable. */
    Assessment capability();

    /** A page of subjects in name order. */
    Subjects subjects(Cursor cursor);

    /**
     * One subject with its versions and compatibility.
     *
     * @throws BrokerException with {@code NOT_FOUND} when the subject is not there
     */
    Subject subject(String name);

    /** What registering a schema would do, and the etag that registration needs. Writes nothing. */
    Preview previewRegister(String subject, SchemaType type, String schema);

    /**
     * Register a schema. A schema the registry already holds returns that version and creates no
     * second one.
     *
     * @throws BrokerException with {@code FAILED_PRECONDITION} when the precondition no longer matches
     */
    SubjectVersion register(String subject, SchemaType type, String schema, Precondition condition);

    /** What a subject's compatibility level would become, and the etag the change needs. Writes nothing. */
    Preview previewCompatibility(String subject, CompatibilityLevel level);

    /** Set one subject's compatibility level. */
    CompatibilityLevel setCompatibility(String subject, CompatibilityLevel level, Precondition condition);

    /** What the registry default would become, and the etag the change needs. Writes nothing. */
    Preview previewCompatibilityDefault(CompatibilityLevel level);

    /** Set the registry default level, which a subject without its own level follows. */
    CompatibilityLevel setCompatibilityDefault(CompatibilityLevel level, Precondition condition);

    /**
     * What deleting a subject would do, and the etag the delete needs. Writes nothing.
     *
     * @param permanent whether this is the irreversible delete; where the registry has none, this
     *     preview is refused with the registry's reason rather than silently doing the other one
     */
    Preview previewDelete(String subject, boolean permanent);

    /**
     * Delete a subject.
     *
     * @param permanent whether to delete the schemas too, which cannot be undone
     */
    void delete(String subject, boolean permanent, Precondition condition);

    /**
     * Decode a payload that carries a schema id.
     *
     * @param serdeName the name of a serde the installation registers, or {@code null} to read the
     *     framing and resolve the id against this registry. A name is never a class name: only a
     *     registered name decodes, and an unknown one is {@code SERDE_NOT_REGISTERED}.
     * @param body the payload as it arrived, framing included
     */
    DecodedRecord decode(String serdeName, MessageBody body);

    /**
     * Encode a record with a registered serde so that decoding it returns the same record.
     *
     * @param serdeName a name the installation registers
     * @param subject the subject to register the schema under
     * @param record the record as maps, lists and scalars
     */
    byte[] encode(String serdeName, String subject, Map<String, ?> record);
}
