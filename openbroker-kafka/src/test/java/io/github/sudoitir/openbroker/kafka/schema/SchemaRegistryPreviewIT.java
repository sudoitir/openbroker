package io.github.sudoitir.openbroker.kafka.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.operation.Precondition;
import io.github.sudoitir.openbroker.operation.Preview;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** A preview writes nothing, and a stale etag is refused without writing. */
class SchemaRegistryPreviewIT {

    private static final String SCHEMA_A =
            "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"city\",\"type\":\"string\"}]}";
    private static final String SCHEMA_B =
            "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"count\",\"type\":\"long\"}]}";

    @BeforeAll
    static void start() {
        SchemaRegistrySupport.start();
    }

    @Test
    void aPreviewAndAStaleRefusalLeaveTheVersionsUnchanged() {
        String name = SchemaRegistrySupport.subject();
        try (ClusterConnection connection = SchemaRegistrySupport.connect(SchemaRegistrySupport.confluent(null))) {
            SchemaCatalog catalog = connection.unwrap(SchemaCatalog.class).orElseThrow();
            List<Integer> before = SchemaRegistrySupport.versions(catalog, name);
            Preview preview = catalog.previewRegister(name, SchemaType.AVRO, SCHEMA_A);
            assertThat(SchemaRegistrySupport.versions(catalog, name)).isEqualTo(before);

            catalog.register(name, SchemaType.AVRO, SCHEMA_B, Precondition.none());
            List<Integer> afterOther = SchemaRegistrySupport.versions(catalog, name);
            assertThatThrownBy(() ->
                            catalog.register(name, SchemaType.AVRO, SCHEMA_A, Precondition.ifMatch(preview.etag())))
                    .isInstanceOf(BrokerException.class)
                    .satisfies(error -> {
                        BrokerException failure = (BrokerException) error;
                        assertThat(failure.status()).isEqualTo(Status.FAILED_PRECONDITION);
                        assertThat(failure.reason()).isEqualTo(ErrorReasons.PRECONDITION_FAILED);
                    });
            assertThat(SchemaRegistrySupport.versions(catalog, name)).isEqualTo(afterOther);
        }
    }

    @Test
    void anAlreadyRegisteredSchemaStatesItsIdAndAddsNoVersion() {
        String name = SchemaRegistrySupport.subject();
        try (ClusterConnection connection = SchemaRegistrySupport.connect(SchemaRegistrySupport.confluent(null))) {
            SchemaCatalog catalog = connection.unwrap(SchemaCatalog.class).orElseThrow();
            SubjectVersion first = catalog.register(name, SchemaType.AVRO, SCHEMA_A, Precondition.none());
            Preview preview = catalog.previewRegister(name, SchemaType.AVRO, SCHEMA_A);
            assertThat(preview.diff())
                    .anySatisfy(change -> assertThat(change.after()).contains(first.id()));
            assertThat(SchemaRegistrySupport.versions(catalog, name)).containsExactly(1);
            catalog.register(name, SchemaType.AVRO, SCHEMA_A, Precondition.ifMatch(preview.etag()));
            assertThat(SchemaRegistrySupport.versions(catalog, name)).containsExactly(1);
        }
    }

    @Test
    void aPermanentDeletePreviewWritesNothing() {
        String name = SchemaRegistrySupport.subject();
        try (ClusterConnection connection = SchemaRegistrySupport.connect(SchemaRegistrySupport.confluent(null))) {
            SchemaCatalog catalog = connection.unwrap(SchemaCatalog.class).orElseThrow();
            catalog.register(name, SchemaType.AVRO, SCHEMA_A, Precondition.none());
            Preview preview = catalog.previewDelete(name, true);
            assertThat(preview.diff())
                    .anySatisfy(
                            change -> assertThat(String.valueOf(change.after().orElse("")))
                                    .contains("cannot be restored"));
            assertThat(SchemaRegistrySupport.versions(catalog, name)).containsExactly(1);
        }
    }
}
