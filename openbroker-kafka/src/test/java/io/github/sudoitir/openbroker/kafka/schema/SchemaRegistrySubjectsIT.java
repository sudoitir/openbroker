package io.github.sudoitir.openbroker.kafka.schema;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.operation.Precondition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** A subject registered on a Confluent-compatible registry is listed with its version. */
class SchemaRegistrySubjectsIT {

    private static final String SCHEMA =
            "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"city\",\"type\":\"string\"}]}";

    @BeforeAll
    static void start() {
        SchemaRegistrySupport.start();
    }

    @Test
    void aRegisteredSubjectIsListedWithItsVersion() {
        String name = SchemaRegistrySupport.subject();
        try (ClusterConnection connection = SchemaRegistrySupport.connect(SchemaRegistrySupport.confluent(null))) {
            SchemaCatalog catalog = connection.unwrap(SchemaCatalog.class).orElseThrow();
            SubjectVersion registered = catalog.register(name, SchemaType.AVRO, SCHEMA, Precondition.none());

            Subject listed = SchemaRegistrySupport.find(catalog, name);
            assertThat(listed).isNotNull();
            assertThat(listed.versions()).extracting(SubjectVersion::id).contains(registered.id());
            assertThat(listed.compatibility().isPresent()
                            || listed.compatibilityNote().isPresent())
                    .isTrue();
        }
    }
}
