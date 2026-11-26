package io.github.sudoitir.openbroker.kafka.schema;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.kafka.KafkaOptions;
import io.github.sudoitir.openbroker.kafka.admin.KafkaCluster;
import io.github.sudoitir.openbroker.messaging.Cursor;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.operation.Precondition;
import io.github.sudoitir.openbroker.operation.Preview;
import java.util.Map;
import java.util.Optional;

/**
 * A Glue registry behind the same catalog. Glue's schema version id is a UUID, not the integer a record carries, and
 * this driver does not invent one, and it does not fall back to the ambient credential chain. Every operation says so
 * rather than answering as if it were a Confluent registry.
 */
final class GlueSchemaRegistry implements SchemaCatalog {

    static final String REASON =
            "Glue identifies a schema version by a UUID, not the integer id a record carries, and this driver does not invent one.";

    private final KafkaCluster cluster;

    GlueSchemaRegistry(KafkaCluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public Assessment capability() {
        return Assessment.unavailable(reason(), Optional.empty());
    }

    @Override
    public Subjects subjects(Cursor cursor) {
        throw unavailable();
    }

    @Override
    public Subject subject(String name) {
        throw unavailable();
    }

    @Override
    public Preview previewRegister(String subject, SchemaType type, String schema) {
        throw unavailable();
    }

    @Override
    public SubjectVersion register(String subject, SchemaType type, String schema, Precondition condition) {
        throw unavailable();
    }

    @Override
    public Preview previewCompatibility(String subject, CompatibilityLevel level) {
        throw unavailable();
    }

    @Override
    public CompatibilityLevel setCompatibility(String subject, CompatibilityLevel level, Precondition condition) {
        throw unavailable();
    }

    @Override
    public Preview previewCompatibilityDefault(CompatibilityLevel level) {
        throw unavailable();
    }

    @Override
    public CompatibilityLevel setCompatibilityDefault(CompatibilityLevel level, Precondition condition) {
        throw unavailable();
    }

    @Override
    public Preview previewDelete(String subject, boolean permanent) {
        throw unavailable();
    }

    @Override
    public void delete(String subject, boolean permanent, Precondition condition) {
        throw unavailable();
    }

    @Override
    public DecodedRecord decode(String serdeName, MessageBody body) {
        if (serdeName != null && !SerdeRegistry.known(serdeName)) {
            return new DecodedRecord.Undecoded(DecodedRecord.SERDE_NOT_REGISTERED);
        }
        if (body.isTombstone()) {
            return new DecodedRecord.Undecoded(DecodedRecord.NO_SCHEMA_ID);
        }
        throw unavailable();
    }

    @Override
    public byte[] encode(String serdeName, String subject, Map<String, ?> record) {
        throw unavailable();
    }

    @Override
    public void close() {}

    private String reason() {
        String region = cluster.option(KafkaOptions.REGISTRY_GLUE_REGION, "");
        String registry = cluster.option(KafkaOptions.REGISTRY_GLUE_REGISTRY, "");
        String credentials = cluster.option(KafkaOptions.REGISTRY_CREDENTIALS, "");
        if (region.isBlank() || registry.isBlank()) {
            return REASON + " registry.glue.region and registry.glue.registry are not set.";
        }
        if (credentials.isBlank()) {
            return REASON + " No credentials are set, and this driver does not use the ambient credential chain.";
        }
        return REASON + " This driver does not use the ambient credential chain.";
    }

    private BrokerException unavailable() {
        return new BrokerException(
                Status.UNIMPLEMENTED, ErrorInfo.of(ErrorReasons.NOT_SUPPORTED), Dispatch.NOT_SENT, null, reason());
    }
}
