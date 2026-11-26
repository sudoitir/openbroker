package io.github.sudoitir.openbroker.kafka.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.admin.KafkaConnection;
import io.github.sudoitir.openbroker.messaging.Cursor;
import io.github.sudoitir.openbroker.operation.Precondition;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
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

            Precondition none = Precondition.none();
            assertThatThrownBy(() -> catalog.register(name, SchemaType.AVRO, BREAKING, none))
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
            String missing = SchemaRegistrySupport.subject();
            assertThatThrownBy(() -> catalog.subject(missing))
                    .isInstanceOf(BrokerException.class)
                    .extracting(error -> ((BrokerException) error).status())
                    .isEqualTo(Status.NOT_FOUND);
        }
    }

    @Test
    void anOperationTheRegistryDoesNotOfferIsUnavailableAndTheListStillAnswers() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            String method = exchange.getRequestMethod();
            byte[] body;
            int status;
            if ("PUT".equals(method)) {
                status = 405;
                body = "{\"message\":\"not offered\"}".getBytes(StandardCharsets.UTF_8);
            } else if (path.endsWith("/subjects") || path.endsWith("/subjects/")) {
                status = 200;
                body = "[\"kept\"]".getBytes(StandardCharsets.UTF_8);
            } else if (path.contains("/versions/1")) {
                status = 200;
                body = "{\"id\":1,\"version\":1,\"schemaType\":\"AVRO\",\"schema\":\"{\\\"type\\\":\\\"string\\\"}\"}"
                        .getBytes(StandardCharsets.UTF_8);
            } else if (path.contains("/versions")) {
                status = 200;
                body = "[1]".getBytes(StandardCharsets.UTF_8);
            } else if (path.contains("/config")) {
                status = 200;
                body = "{\"compatibilityLevel\":\"NONE\"}".getBytes(StandardCharsets.UTF_8);
            } else {
                status = 404;
                body = "{}".getBytes(StandardCharsets.UTF_8);
            }
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try (KafkaConnection connection =
                SchemaRegistryNoteTest.connection(server.getAddress().getPort())) {
            SchemaCatalog catalog = connection.unwrap(SchemaCatalog.class).orElseThrow();
            assertThat(catalog.subjects(new Cursor("")).items())
                    .extracting(Subject::name)
                    .contains("kept");
            Precondition none = Precondition.none();
            assertThatThrownBy(() -> catalog.setCompatibility("kept", CompatibilityLevel.FULL, none))
                    .isInstanceOf(BrokerException.class)
                    .satisfies(error -> {
                        BrokerException failure = (BrokerException) error;
                        assertThat(failure.status()).isEqualTo(Status.UNIMPLEMENTED);
                        assertThat(failure.getMessage()).contains("not offered");
                    });
            assertThat(catalog.subjects(new Cursor("")).items())
                    .extracting(Subject::name)
                    .contains("kept");
        } finally {
            server.stop(0);
        }
    }
}
