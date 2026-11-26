package io.github.sudoitir.openbroker.kafka.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The three serdes round-trip a record without a registry. Framing is what a decode reads back. */
class SchemaSerdeTest {

    @Test
    void avroProtobufAndJsonRoundTrip() {
        Map<String, Object> record = Map.of("city", "Shiraz", "count", 3L, "open", true);
        for (String name : new String[] {SerdeRegistry.AVRO, SerdeRegistry.PROTOBUF, SerdeRegistry.JSON}) {
            String schema = SerdeRegistry.schemaOf(name, record);
            byte[] framed = SerdeRegistry.frame(name, 7, schema, record);
            assertThat(framed[0]).isZero();
            DecodedRecord decoded = SerdeRegistry.decode(
                    name,
                    SerdeRegistry.typeOf(name),
                    7,
                    "orders-value",
                    schema,
                    Arrays.copyOfRange(framed, 5, framed.length));
            assertThat(decoded).isInstanceOf(DecodedRecord.Decoded.class);
            assertThat(((DecodedRecord.Decoded) decoded).record()).isEqualTo(record);
        }
    }

    @Test
    void aClassNameIsNotASerde() {
        assertThat(SerdeRegistry.known("com.example.Order")).isFalse();
    }
}
