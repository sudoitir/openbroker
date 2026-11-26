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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The catalog a connection holds. The kind is read on each call, so a reconfigure is the next call's registry.
 * Callers reach it through {@code unwrap}, not through this class.
 */
public final class SchemaCatalogs {

    static final String NO_REGISTRY = "No schema registry is configured on this connection";

    private SchemaCatalogs() {}

    public static SchemaCatalog open(KafkaCluster cluster) {
        return new Bound(cluster);
    }

    private static final class Bound implements SchemaCatalog {

        private final KafkaCluster cluster;
        private final ConfluentSchemaRegistry confluent;
        private final Absent absent = new Absent();

        private Bound(KafkaCluster cluster) {
            this.cluster = cluster;
            this.confluent = new ConfluentSchemaRegistry(cluster);
        }

        private SchemaCatalog current() {
            String kind = cluster.option(KafkaOptions.REGISTRY_KIND, KafkaOptions.REGISTRY_NONE);
            if (KafkaOptions.REGISTRY_CONFLUENT.equals(kind)) {
                return confluent;
            }
            if (kind.isBlank() || KafkaOptions.REGISTRY_NONE.equals(kind)) {
                return absent;
            }
            return new Unspoken(kind);
        }

        @Override
        public Assessment capability() {
            return current().capability();
        }

        @Override
        public Subjects subjects(Cursor cursor) {
            return current().subjects(cursor);
        }

        @Override
        public Subject subject(String name) {
            return current().subject(name);
        }

        @Override
        public Preview previewRegister(String subject, SchemaType type, String schema) {
            return current().previewRegister(subject, type, schema);
        }

        @Override
        public SubjectVersion register(String subject, SchemaType type, String schema, Precondition condition) {
            return current().register(subject, type, schema, condition);
        }

        @Override
        public Preview previewCompatibility(String subject, CompatibilityLevel level) {
            return current().previewCompatibility(subject, level);
        }

        @Override
        public CompatibilityLevel setCompatibility(String subject, CompatibilityLevel level, Precondition condition) {
            return current().setCompatibility(subject, level, condition);
        }

        @Override
        public Preview previewCompatibilityDefault(CompatibilityLevel level) {
            return current().previewCompatibilityDefault(level);
        }

        @Override
        public CompatibilityLevel setCompatibilityDefault(CompatibilityLevel level, Precondition condition) {
            return current().setCompatibilityDefault(level, condition);
        }

        @Override
        public Preview previewDelete(String subject, boolean permanent) {
            return current().previewDelete(subject, permanent);
        }

        @Override
        public void delete(String subject, boolean permanent, Precondition condition) {
            current().delete(subject, permanent, condition);
        }

        @Override
        public DecodedRecord decode(String serdeName, MessageBody body) {
            return current().decode(serdeName, body);
        }

        @Override
        public byte[] encode(String serdeName, String subject, Map<String, ?> record) {
            return current().encode(serdeName, subject, record);
        }

        @Override
        public void close() {
            confluent.close();
        }
    }

    /** A connection with no registry. It invents no subject. */
    private static final class Absent implements SchemaCatalog {

        @Override
        public Assessment capability() {
            return Assessment.unavailable(NO_REGISTRY, Optional.empty());
        }

        @Override
        public Subjects subjects(Cursor cursor) {
            Objects.requireNonNull(cursor, "cursor");
            return new Subjects(List.of(), Optional.empty());
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
            return new DecodedRecord.Undecoded(DecodedRecord.NO_REGISTRY);
        }

        @Override
        public byte[] encode(String serdeName, String subject, Map<String, ?> record) {
            throw unavailable();
        }

        @Override
        public void close() {
            // Nothing to release: this catalog holds no client.
        }

        private static BrokerException unavailable() {
            return new BrokerException(
                    Status.FAILED_PRECONDITION,
                    ErrorInfo.of(ErrorReasons.NOT_SUPPORTED),
                    Dispatch.NOT_SENT,
                    null,
                    NO_REGISTRY);
        }
    }

    /** A kind this driver does not speak. It is not reported as if no registry were configured. */
    private static final class Unspoken implements SchemaCatalog {

        private final String kind;

        private Unspoken(String kind) {
            this.kind = kind;
        }

        private String reason() {
            return "This driver does not speak a registry kind of " + kind + ".";
        }

        @Override
        public Assessment capability() {
            return Assessment.unavailable(reason(), Optional.empty());
        }

        @Override
        public Subjects subjects(Cursor cursor) {
            throw refused();
        }

        @Override
        public Subject subject(String name) {
            throw refused();
        }

        @Override
        public Preview previewRegister(String subject, SchemaType type, String schema) {
            throw refused();
        }

        @Override
        public SubjectVersion register(String subject, SchemaType type, String schema, Precondition condition) {
            throw refused();
        }

        @Override
        public Preview previewCompatibility(String subject, CompatibilityLevel level) {
            throw refused();
        }

        @Override
        public CompatibilityLevel setCompatibility(String subject, CompatibilityLevel level, Precondition condition) {
            throw refused();
        }

        @Override
        public Preview previewCompatibilityDefault(CompatibilityLevel level) {
            throw refused();
        }

        @Override
        public CompatibilityLevel setCompatibilityDefault(CompatibilityLevel level, Precondition condition) {
            throw refused();
        }

        @Override
        public Preview previewDelete(String subject, boolean permanent) {
            throw refused();
        }

        @Override
        public void delete(String subject, boolean permanent, Precondition condition) {
            throw refused();
        }

        @Override
        public DecodedRecord decode(String serdeName, MessageBody body) {
            if (serdeName != null && !SerdeRegistry.known(serdeName)) {
                return new DecodedRecord.Undecoded(DecodedRecord.SERDE_NOT_REGISTERED);
            }
            if (body.isTombstone()) {
                return new DecodedRecord.Undecoded(DecodedRecord.NO_SCHEMA_ID);
            }
            throw refused();
        }

        @Override
        public byte[] encode(String serdeName, String subject, Map<String, ?> record) {
            throw refused();
        }

        @Override
        public void close() {
            // Nothing to release: this catalog holds no client.
        }

        private BrokerException refused() {
            return new BrokerException(
                    Status.INVALID_ARGUMENT,
                    ErrorInfo.of(ErrorReasons.INVALID_ARGUMENT),
                    Dispatch.NOT_SENT,
                    null,
                    reason());
        }
    }
}
