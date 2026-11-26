package io.github.sudoitir.openbroker.kafka.schema;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Encoding a record and decoding the bytes returns the record, for each registered serde. */
class SchemaRoundTripIT {

    @BeforeAll
    static void start() {
        SchemaRegistrySupport.start();
    }

    @Test
    void avroProtobufAndJsonReadBack() {
        Map<String, Object> record = Map.of("city", "Shiraz", "count", 3L, "open", true);
        try (ClusterConnection connection = SchemaRegistrySupport.connect(SchemaRegistrySupport.confluent(null))) {
            SchemaCatalog catalog = connection.unwrap(SchemaCatalog.class).orElseThrow();
            for (String serde : new String[] {"avro", "protobuf", "json"}) {
                byte[] framed = catalog.encode(serde, SchemaRegistrySupport.subject(), record);
                DecodedRecord decoded = catalog.decode(serde, MessageBody.of(framed));
                assertThat(decoded).isInstanceOf(DecodedRecord.Decoded.class);
                assertThat(((DecodedRecord.Decoded) decoded).record()).isEqualTo(record);
            }
            Map<String, Object> withBytes = Map.of("blob", new byte[] {4, 5});
            byte[] framed = catalog.encode("avro", SchemaRegistrySupport.subject(), withBytes);
            DecodedRecord decoded = catalog.decode("avro", MessageBody.of(framed));
            assertThat((byte[]) ((Map<?, ?>) ((DecodedRecord.Decoded) decoded).record()).get("blob"))
                    .isEqualTo(new byte[] {4, 5});
        }
    }
}
