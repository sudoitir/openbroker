package io.github.sudoitir.openbroker.artemis.internal;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;

/** A local HTTP server that answers each request with the next queued response and keeps what it was sent. */
final class FixtureServer implements AutoCloseable {

    record Answer(int status, Map<String, String> headers, String body) {}

    record Received(String method, Map<String, List<String>> headers, String body) {}

    private final HttpServer server;
    private final Deque<Answer> answers = new ArrayDeque<>();
    private final List<Received> received = new ArrayList<>();

    FixtureServer() {
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        server.createContext("/", this::answer);
        server.start();
    }

    URI endpoint() {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/console/jolokia");
    }

    FixtureServer json(String fixture) {
        return respond(200, Map.of("Content-Type", "application/json"), fixture(fixture));
    }

    FixtureServer respond(int status, Map<String, String> headers, String body) {
        synchronized (answers) {
            answers.add(new Answer(status, headers, body));
        }
        return this;
    }

    List<Received> received() {
        synchronized (answers) {
            return List.copyOf(received);
        }
    }

    static String fixture(String name) {
        try (InputStream in = FixtureServer.class.getResourceAsStream("/jolokia/" + name)) {
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
            Answer answer;
            synchronized (answers) {
                received.add(new Received(exchange.getRequestMethod(), Map.copyOf(exchange.getRequestHeaders()), body));
                answer = answers.poll();
            }
            if (answer == null) {
                exchange.sendResponseHeaders(500, -1);
                return;
            }
            answer.headers()
                    .forEach((name, value) -> exchange.getResponseHeaders().add(name, value));
            byte[] out = answer.body().getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(answer.status(), out.length == 0 ? -1 : out.length);
            if (out.length > 0) {
                exchange.getResponseBody().write(out);
            }
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
