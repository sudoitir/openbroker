package io.github.sudoitir.openbroker.kafka.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.operation.Precondition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** A schema the subject's compatibility rejects is not registered. */
class SchemaRegistryRefusalIT {

    private static final String FIRST =
            "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"city\",\"type\":\"string\"}]}";
    private static final String BREAKING =
            "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"count\",\"type\":\"long\"}]}";

    @BeforeAll
    static void start() {
        SchemaRegistrySupport.start();
    }

    @Test
    void anIncompatibleSchemaIsRefusedAndAddsNoVersion() {
        String name = SchemaRegistrySupport.subject();
        try (ClusterConnection connection = SchemaRegistrySupport.connect(SchemaRegistrySupport.confluent(null))) {
            SchemaCatalog catalog = connection.unwrap(SchemaCatalog.class).orElseThrow();
            catalog.register(name, SchemaType.AVRO, FIRST, Precondition.none());
            catalog.setCompatibility(name, CompatibilityLevel.FULL, Precondition.none());

            assertThatThrownBy(() -> catalog.register(name, SchemaType.AVRO, BREAKING, Precondition.none()))
                    .isInstanceOf(BrokerException.class)
                    .extracting(error -> ((BrokerException) error).status())
                    .isEqualTo(Status.FAILED_PRECONDITION);
            assertThat(SchemaRegistrySupport.versions(catalog, name)).containsExactly(1);
        }
    }

    @Test
    void aMissingSubjectIsNotFound() {
        try (ClusterConnection connection = SchemaRegistrySupport.connect(SchemaRegistrySupport.confluent(null))) {
            SchemaCatalog catalog = connection.unwrap(SchemaCatalog.class).orElseThrow();
            assertThatThrownBy(() -> catalog.subject(SchemaRegistrySupport.subject()))
                    .isInstanceOf(BrokerException.class)
                    .extracting(error -> ((BrokerException) error).status())
                    .isEqualTo(Status.NOT_FOUND);
        }
    }
}
