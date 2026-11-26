package io.github.sudoitir.openbroker.kafka.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.kafka.KafkaOptions;
import io.github.sudoitir.openbroker.messaging.Cursor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** A registry credential is absent from responses, exceptions and logs. */
class SchemaCredentialLeakIT {

    private static final String SECRET = "registry-secret-do-not-leak";

    @BeforeAll
    static void start() {
        SchemaRegistrySupport.start();
    }

    @Test
    void theSecretIsAbsentFromResponsesExceptionsAndLogs() {
        List<String> lines = new ArrayList<>();
        Logger logger = Logger.getLogger(RegistryHttp.class.getName());
        Handler handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                lines.add(record.getMessage());
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
        };
        handler.setLevel(Level.ALL);
        logger.setLevel(Level.ALL);
        logger.addHandler(handler);
        try {
            try (ClusterConnection connection =
                    SchemaRegistrySupport.connect(SchemaRegistrySupport.confluent("user:" + SECRET))) {
                SchemaCatalog catalog = connection.unwrap(SchemaCatalog.class).orElseThrow();
                assertThat(catalog.toString()).doesNotContain(SECRET);
                assertThat(catalog.subjects(new Cursor("")).toString()).doesNotContain(SECRET);
                assertThatThrownBy(() -> catalog.subject("missing-" + SECRET))
                        .isInstanceOf(BrokerException.class)
                        .satisfies(error -> assertThat(error.toString()).doesNotContain(SECRET));
            }
            try (ClusterConnection connection = SchemaRegistrySupport.connect(Map.of(
                    KafkaOptions.REGISTRY_KIND,
                    KafkaOptions.REGISTRY_CONFLUENT,
                    KafkaOptions.REGISTRY_URL,
                    "http://127.0.0.1:1",
                    KafkaOptions.REGISTRY_CREDENTIALS,
                    "user:" + SECRET))) {
                SchemaCatalog catalog = connection.unwrap(SchemaCatalog.class).orElseThrow();
                assertThatThrownBy(() -> catalog.subjects(new Cursor("")))
                        .isInstanceOf(BrokerException.class)
                        .satisfies(error -> assertThat(error.toString()).doesNotContain(SECRET));
            }
            assertThat(lines).isNotEmpty();
            assertThat(lines).noneMatch(line -> line.contains(SECRET));
            assertThat(RegistryHttp.Redactor.of("user", SECRET).apply("failed " + SECRET + " token"))
                    .doesNotContain(SECRET);
        } finally {
            logger.removeHandler(handler);
        }
    }
}
