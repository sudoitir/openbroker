package io.github.sudoitir.openbroker.rabbitmq.admin;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A local stand-in for the management API: it serves canned JSON from {@code rabbitmq/management/} (or any status
 * and body) per method and path, in the order queued with the last answer repeating, and records what it was sent.
 */
final class FakeManagement implements AutoCloseable {

    record Answer(int status, Map<String, String> headers, byte[] body, boolean hold) {}

    record Received(String method, String path, String query, Map<String, List<String>> headers, String body) {
        String header(String name) {
            return headers.entrySet().stream()
                    .filter(e -> e.getKey().equalsIgnoreCase(name))
                    .map(e -> e.getValue().getFirst())
                    .findFirst()
                    .orElse(null);
        }
    }

    private final HttpServer server;
    private final Map<String, Deque<Answer>> routes = new HashMap<>();
    private final List<Received> received = new ArrayList<>();
    private final CountDownLatch release = new CountDownLatch(1);

    FakeManagement() {
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        server.createContext("/", this::answer);
        server.start();
    }

    /** The base URL of this server, for a management endpoint. */
    String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    Endpoint endpoint() {
        return Endpoint.of(Plane.MANAGEMENT, "http", url());
    }

    FakeManagement json(String method, String path, String fixture) {
        return respond(method, path, 200, Map.of("Content-Type", "application/json"), fixture(fixture));
    }

    FakeManagement respond(String method, String path, int status, Map<String, String> headers, String body) {
        return add(method, path, new Answer(status, headers, body.getBytes(StandardCharsets.UTF_8), false));
    }

    /** An answer that is only given once the server closes, so the caller's timeout fires first. */
    FakeManagement hang(String method, String path) {
        return add(method, path, new Answer(200, Map.of(), new byte[0], true));
    }

    /** An answer whose headers arrive and whose body stops halfway until the server closes. */
    FakeManagement stallBody(String method, String path) {
        return add(method, path, new Answer(200, Map.of(), "{\"name\":".getBytes(StandardCharsets.UTF_8), true));
    }

    private FakeManagement add(String method, String path, Answer answer) {
        synchronized (routes) {
            routes.computeIfAbsent(method + " " + path, k -> new ArrayDeque<>()).add(answer);
        }
        return this;
    }

    List<Received> received() {
        synchronized (routes) {
            return List.copyOf(received);
        }
    }

    static String fixture(String name) {
        try (InputStream in = FakeManagement.class.getResourceAsStream("/rabbitmq/management/" + name)) {
            if (in == null) {
                throw new IllegalArgumentException("No fixture " + name);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void answer(HttpExchange exchange) throws IOException {
        try (exchange) {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            String path = exchange.getRequestURI().getRawPath();
            Answer answer;
            synchronized (routes) {
                received.add(new Received(
                        exchange.getRequestMethod(),
                        path,
                        exchange.getRequestURI().getRawQuery(),
                        Map.copyOf(exchange.getRequestHeaders()),
                        body));
                Deque<Answer> queue = routes.get(exchange.getRequestMethod() + " " + path);
                answer = queue == null ? null : queue.size() > 1 ? queue.poll() : queue.peek();
            }
            if (answer == null) {
                exchange.sendResponseHeaders(404, -1);
                return;
            }
            if (answer.hold()) {
                if (answer.body().length > 0) {
                    // The headers and the start of a longer body, then nothing until the server closes.
                    exchange.sendResponseHeaders(answer.status(), answer.body().length * 2L);
                    exchange.getResponseBody().write(answer.body());
                    exchange.getResponseBody().flush();
                }
                try {
                    if (!release.await(30, TimeUnit.SECONDS)) {
                        exchange.close();
                    }
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                }
                return;
            }
            answer.headers().forEach((k, v) -> exchange.getResponseHeaders().add(k, v));
            if (answer.body().length == 0) {
                exchange.sendResponseHeaders(answer.status(), -1);
            } else {
                exchange.sendResponseHeaders(answer.status(), answer.body().length);
                exchange.getResponseBody().write(answer.body());
            }
        }
    }

    @Override
    public void close() {
        release.countDown();
        server.stop(0);
    }

    /** A cluster over {@code endpoints} (as cluster endpoints) with the given credentials and options. */
    static RabbitMqCluster cluster(Credentials credentials, ConnectOptions options, Endpoint... endpoints) {
        return new RabbitMqCluster(
                new ClusterSpec(List.of(), List.of(endpoints), Map.of()),
                CredentialSource.of(credentials),
                new TestContext(options));
    }

    static RabbitMqCluster cluster(Endpoint... endpoints) {
        return cluster(Credentials.usernamePassword("ops", "s3cret"), ConnectOptions.defaults(), endpoints);
    }

    /** A {@link DriverContext} that keeps shared objects in a map and closes them with itself. */
    static final class TestContext implements DriverContext {
        private final ConnectOptions options;
        private final Map<String, AutoCloseable> shared = new ConcurrentHashMap<>();

        TestContext(ConnectOptions options) {
            this.options = options;
        }

        @Override
        public ConnectOptions options() {
            return options;
        }

        @Override
        public <T extends AutoCloseable> T shared(String key, Class<T> type, Supplier<T> factory) {
            return type.cast(shared.computeIfAbsent(key, k -> factory.get()));
        }

        @Override
        public void evict(String key) {
            AutoCloseable removed = shared.remove(key);
            if (removed != null) {
                try {
                    removed.close();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }
}
