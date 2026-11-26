package io.github.sudoitir.openbroker.kafka.schema;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.kafka.KafkaDriver;
import io.github.sudoitir.openbroker.kafka.KafkaOptions;
import io.github.sudoitir.openbroker.kafka.admin.KafkaCluster;
import io.github.sudoitir.openbroker.kafka.admin.KafkaConnection;
import io.github.sudoitir.openbroker.operation.Preview;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/** A registry with no compatibility endpoint still previews without writing. */
class SchemaRegistryNoteTest {

    @Test
    void aMissingCompatibilityEndpointWritesNothingAndSaysTheCheckHappensOnApply() throws Exception {
        AtomicBoolean wrote = new AtomicBoolean();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            String method = exchange.getRequestMethod();
            if ("POST".equals(method) && path.endsWith("/versions")) {
                wrote.set(true);
            }
            byte[] body;
            int status;
            if (path.contains("/compatibility/")) {
                status = 405;
                body = "no".getBytes(StandardCharsets.UTF_8);
            } else if ("GET".equals(method) && path.endsWith("/versions")) {
                status = 404;
                body = "{\"error_code\":40401,\"message\":\"Subject not found.\"}".getBytes(StandardCharsets.UTF_8);
            } else if ("POST".equals(method)) {
                status = 404;
                body = "{\"error_code\":40403,\"message\":\"Schema not found\"}".getBytes(StandardCharsets.UTF_8);
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
        try (KafkaConnection connection = connection(server.getAddress().getPort())) {
            SchemaCatalog catalog = connection.unwrap(SchemaCatalog.class).orElseThrow();
            Preview preview = catalog.previewRegister(
                    "orders-value",
                    SchemaType.AVRO,
                    "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"city\",\"type\":\"string\"}]}");
            assertThat(preview.diff())
                    .anySatisfy(
                            change -> assertThat(String.valueOf(change.after().orElse("")))
                                    .contains("happens on apply"));
            assertThat(wrote).isFalse();
        } finally {
            server.stop(0);
        }
    }

    static KafkaConnection connection(int port) {
        return connection(Map.of(
                KafkaOptions.REGISTRY_KIND,
                KafkaOptions.REGISTRY_CONFLUENT,
                KafkaOptions.REGISTRY_URL,
                "http://127.0.0.1:" + port));
    }

    static KafkaConnection connection(Map<String, String> registryOptions) {
        return connection(new ClusterSpec(
                List.of(new NodeSpec(
                        NodeId.of("1"), List.of(Endpoint.of(Plane.DATA, KafkaOptions.PROTOCOL, "127.0.0.1:1")))),
                List.of(),
                registryOptions));
    }

    static KafkaConnection connection(ClusterSpec spec) {
        ConnectOptions options = ConnectOptions.defaults();
        DriverContext context = new DriverContext() {
            @Override
            public ConnectOptions options() {
                return options;
            }

            @Override
            public <T extends AutoCloseable> T shared(String key, Class<T> type, Supplier<T> factory) {
                return factory.get();
            }

            @Override
            public void evict(String key) {}
        };
        return new KafkaConnection(
                new KafkaDriver().system(), new KafkaCluster(spec, CredentialSource.none(), context), context);
    }
}
