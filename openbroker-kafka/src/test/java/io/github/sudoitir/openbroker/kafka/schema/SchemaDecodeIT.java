package io.github.sudoitir.openbroker.kafka.schema;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.operation.Precondition;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** What does not decode is named, and never presented as an empty record. */
class SchemaDecodeIT {

    @BeforeAll
    static void start() {
        SchemaRegistrySupport.start();
    }

    @Test
    void aTombstoneIsNoSchemaId() {
        try (ClusterConnection connection = SchemaRegistrySupport.connect(SchemaRegistrySupport.confluent(null))) {
            SchemaCatalog catalog = connection.unwrap(SchemaCatalog.class).orElseThrow();
            assertThat(reason(catalog.decode(null, MessageBody.tombstone()))).isEqualTo(DecodedRecord.NO_SCHEMA_ID);
        }
    }

    @Test
    void aClassNameIsNotRegistered() {
        try (ClusterConnection connection = SchemaRegistrySupport.connect(SchemaRegistrySupport.confluent(null))) {
            SchemaCatalog catalog = connection.unwrap(SchemaCatalog.class).orElseThrow();
            DecodedRecord decoded = catalog.decode("com.example.Order", MessageBody.of(new byte[] {0, 0, 0, 0, 1}));
            assertThat(reason(decoded)).isEqualTo(DecodedRecord.SERDE_NOT_REGISTERED);
        }
    }

    @Test
    void anUnknownIdAndACutShortBodyAreUndecoded() {
        try (ClusterConnection connection = SchemaRegistrySupport.connect(SchemaRegistrySupport.confluent(null))) {
            SchemaCatalog catalog = connection.unwrap(SchemaCatalog.class).orElseThrow();
            byte[] unknown =
                    ByteBuffer.allocate(5).put((byte) 0).putInt(999_999).array();
            assertThat(reason(catalog.decode(null, MessageBody.of(unknown)))).isEqualTo(DecodedRecord.SCHEMA_NOT_FOUND);
            assertThat(reason(catalog.decode(null, MessageBody.of(new byte[] {1, 2, 3}))))
                    .isEqualTo(DecodedRecord.NO_SCHEMA_ID);
            assertThat(reason(catalog.decode(null, MessageBody.of(new byte[] {0, 0, 1}))))
                    .isEqualTo(DecodedRecord.TRUNCATED);
        }
    }

    @Test
    void aSerdeThatRefusesThePayloadIsNamed() {
        String schema = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"city\",\"type\":\"string\"}]}";
        try (ClusterConnection connection = SchemaRegistrySupport.connect(SchemaRegistrySupport.confluent(null))) {
            SchemaCatalog catalog = connection.unwrap(SchemaCatalog.class).orElseThrow();
            SubjectVersion version =
                    catalog.register(SchemaRegistrySupport.subject(), SchemaType.AVRO, schema, Precondition.none());
            byte[] garbage = ByteBuffer.allocate(7)
                    .put((byte) 0)
                    .putInt(version.id())
                    .put((byte) 0xff)
                    .put((byte) 0xff)
                    .array();
            assertThat(reason(catalog.decode("avro", MessageBody.of(garbage)))).isEqualTo(DecodedRecord.SERDE_REFUSED);
        }
    }

    private static String reason(DecodedRecord decoded) {
        assertThat(decoded).isInstanceOf(DecodedRecord.Undecoded.class);
        return ((DecodedRecord.Undecoded) decoded).reason();
    }
}
