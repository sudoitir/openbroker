package io.github.sudoitir.openbroker.kafka.schema;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.kafka.KafkaOptions;
import io.github.sudoitir.openbroker.messaging.Cursor;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** No registry is unavailable. A named one that has not been called is unknown, not unavailable. */
class SchemaCatalogUnavailableIT {

    @BeforeAll
    static void start() {
        SchemaRegistrySupport.start();
    }

    @Test
    void noRegistryIsUnavailableAndInventsNoSubject() {
        try (ClusterConnection connection = SchemaRegistrySupport.connect(Map.of())) {
            Assessment assessment = connection.access().probe().of(SchemaCatalog.SCHEMA_REGISTRY);
            assertThat(assessment.status()).isEqualTo(Assessment.Status.UNAVAILABLE);
            assertThat(assessment.reason()).isEqualTo(SchemaCatalogs.NO_REGISTRY);
            SchemaCatalog catalog = connection.unwrap(SchemaCatalog.class).orElseThrow();
            assertThat(catalog.capability().status()).isEqualTo(Assessment.Status.UNAVAILABLE);
            assertThat(catalog.subjects(new Cursor("")).items()).isEmpty();
            DecodedRecord decoded = catalog.decode(null, MessageBody.of(new byte[] {1, 2, 3}));
            assertThat(decoded).isInstanceOf(DecodedRecord.Undecoded.class);
            assertThat(((DecodedRecord.Undecoded) decoded).reason()).isEqualTo(DecodedRecord.NO_REGISTRY);
        }
    }

    @Test
    void aNamedRegistryIsUnknownUntilReached() {
        try (ClusterConnection connection =
                SchemaRegistrySupport.connect(Map.of(KafkaOptions.REGISTRY_KIND, KafkaOptions.REGISTRY_CONFLUENT))) {
            Assessment assessment = connection.access().probe().of(SchemaCatalog.SCHEMA_REGISTRY);
            assertThat(assessment.status()).isEqualTo(Assessment.Status.UNKNOWN);
        }
    }

    @Test
    void aKindOtherThanNoneOrConfluentIsRefused() {
        assertThat(KafkaOptions.SCHEMA.validate(Map.of(KafkaOptions.REGISTRY_KIND, "other")))
                .isNotEmpty()
                .allSatisfy(violation -> assertThat(violation.detail()).contains("must be one of"));
    }
}
