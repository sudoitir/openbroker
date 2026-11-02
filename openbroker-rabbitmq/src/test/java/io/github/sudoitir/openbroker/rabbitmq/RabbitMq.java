package io.github.sudoitir.openbroker.rabbitmq;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * The management API as the integration tests use it to set a cluster up and to look at it from outside: users,
 * permissions, queues, exchanges, bindings, connections. It speaks plain HTTP with the JDK's client, so a test
 * never depends on the driver it exercises to arrange what it tests.
 */
public final class RabbitMq {

    private static final ObjectMapper JSON = JsonMapper.builder().build();
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration POLL = Duration.ofMillis(100);

    private final String baseUrl;
    private final String authorization;
    private final HttpClient client;

    private RabbitMq(String baseUrl, String user, String password, SSLContext tls) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.authorization =
                "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
        HttpClient.Builder builder =
                HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(10));
        if (tls != null) {
            builder.sslContext(tls);
        }
        this.client = builder.build();
    }

    /** The management API at {@code baseUrl} (such as {@code http://localhost:15672}), signed in as the user. */
    public static RabbitMq at(String baseUrl, String user, String password) {
        return new RabbitMq(baseUrl, user, password, null);
    }

    /** The management API at an {@code https} {@code baseUrl}, trusting what {@code tls} trusts. */
    public static RabbitMq at(String baseUrl, String user, String password, SSLContext tls) {
        return new RabbitMq(baseUrl, user, password, tls);
    }

    /** The management API of node {@code index} of {@code cluster}, counted from 0, signed in as its admin. */
    public static RabbitMq of(RabbitMqTestCluster cluster, int index) {
        return at(cluster.managementUrl(index), RabbitMqTestCluster.ADMIN_USER, RabbitMqTestCluster.ADMIN_PASSWORD);
    }

    /** The image the tests run on: the {@code rabbitmq.image} system property, or the newest the driver is tested on. */
    public static String image() {
        return RabbitMqTestCluster.image();
    }

    /** {@code value} as one path segment ({@code /} becomes {@code %2F}). */
    public static String segment(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    /** Sends {@code method} to {@code path} (such as {@code /api/queues}) with an optional JSON body. */
    public HttpResponse<String> send(String method, String path, Object body) {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(REQUEST_TIMEOUT)
                .header("Authorization", authorization)
                .header("Accept", "application/json");
        if (body == null) {
            request.method(method, BodyPublishers.noBody());
        } else {
            request.header("Content-Type", "application/json")
                    .method(method, BodyPublishers.ofString(JSON.writeValueAsString(body)));
        }
        try {
            return client.send(request.build(), BodyHandlers.ofString());
        } catch (IOException e) {
            throw new IllegalStateException(method + " " + path + " failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(method + " " + path + " was interrupted", e);
        }
    }

    /** Sends the request and fails unless the answer is a success. */
    private JsonNode call(String method, String path, Object body) {
        HttpResponse<String> response = send(method, path, body);
        if (response.statusCode() / 100 != 2) {
            throw new IllegalStateException(
                    method + " " + path + " answered " + response.statusCode() + ": " + response.body());
        }
        String text = response.body();
        return text == null || text.isBlank() ? JSON.createObjectNode() : JSON.readTree(text);
    }

    /** The JSON at {@code path}. */
    public JsonNode get(String path) {
        return call("GET", path, null);
    }

    /** Whether {@code path} answers a success. */
    public boolean exists(String path) {
        return send("GET", path, null).statusCode() / 100 == 2;
    }

    /** Makes a user with a password and tags such as {@code management} or {@code administrator}. */
    public void createUser(String name, String password, String tags) {
        call("PUT", "/api/users/" + segment(name), Map.of("password", password, "tags", tags));
    }

    /** Makes a user with no password, which can only sign in with its certificate (EXTERNAL). */
    public void createCertificateUser(String name, String tags) {
        call("PUT", "/api/users/" + segment(name), Map.of("password_hash", "", "tags", tags));
    }

    /** Gives {@code user} the permissions on {@code vhost}, as the three regular expressions. */
    public void setPermissions(String vhost, String user, String configure, String write, String read) {
        call(
                "PUT",
                "/api/permissions/" + segment(vhost) + "/" + segment(user),
                Map.of("configure", configure, "write", write, "read", read));
    }

    /** Makes a user who may do everything on {@code vhost} and whose tag is {@code management}. */
    public void createUserWithAllPermissions(String vhost, String name, String password) {
        createUser(name, password, "management");
        setPermissions(vhost, name, ".*", ".*", ".*");
    }

    /** Makes the virtual host {@code name}. */
    public void createVhost(String name) {
        call("PUT", "/api/vhosts/" + segment(name), Map.of());
    }

    /** Declares a durable queue of {@code type} ({@code classic}, {@code quorum} or {@code stream}). */
    public void declareQueue(String vhost, String name, String type) {
        call(
                "PUT",
                "/api/queues/" + segment(vhost) + "/" + segment(name),
                Map.of("durable", true, "auto_delete", false, "arguments", Map.of("x-queue-type", type)));
    }

    /** Declares a durable queue of {@code type} with the extra queue {@code arguments}. */
    public void declareQueue(String vhost, String name, String type, Map<String, Object> arguments) {
        Map<String, Object> all = new HashMap<>(arguments);
        all.put("x-queue-type", type);
        call(
                "PUT",
                "/api/queues/" + segment(vhost) + "/" + segment(name),
                Map.of("durable", true, "auto_delete", false, "arguments", all));
    }

    /** Deletes the queue, whatever it holds. */
    public void deleteQueue(String vhost, String name) {
        call("DELETE", "/api/queues/" + segment(vhost) + "/" + segment(name), null);
    }

    /** Declares a durable exchange of {@code type} ({@code direct}, {@code fanout}, {@code topic}, ...). */
    public void declareExchange(String vhost, String name, String type) {
        call(
                "PUT",
                "/api/exchanges/" + segment(vhost) + "/" + segment(name),
                Map.of("type", type, "durable", true, "auto_delete", false, "internal", false));
    }

    /** Binds the queue to the exchange with the routing key. */
    public void bindQueue(String vhost, String exchange, String queue, String routingKey) {
        call(
                "POST",
                "/api/bindings/" + segment(vhost) + "/e/" + segment(exchange) + "/q/" + segment(queue),
                Map.of("routing_key", routingKey));
    }

    /** Publishes a text message through the management API; whether it was routed to a queue. */
    public boolean publish(String vhost, String exchange, String routingKey, String payload) {
        JsonNode answer = call(
                "POST",
                "/api/exchanges/" + segment(vhost) + "/" + segment(exchange) + "/publish",
                Map.of(
                        "properties",
                        Map.of(),
                        "routing_key",
                        routingKey,
                        "payload",
                        payload,
                        "payload_encoding",
                        "string"));
        return answer.path("routed").asBoolean(false);
    }

    /** The number of messages ready in the queue, from its listing. */
    public long messagesReady(String vhost, String queue) {
        return get("/api/queues/" + segment(vhost) + "/" + segment(queue))
                .path("messages_ready")
                .asLong(0);
    }

    /** The names of the connections, as the management API lists them. */
    public List<String> connectionNames() {
        List<String> names = new ArrayList<>();
        get("/api/connections").forEach(c -> names.add(c.path("name").asString()));
        return names;
    }

    /** Closes the connection called {@code name} from the broker's side, as an operator or a fault would. */
    public void closeConnection(String name) {
        call("DELETE", "/api/connections/" + segment(name), null);
    }

    /** The number of nodes the cluster lists as running. */
    public long runningNodes() {
        long running = 0;
        for (JsonNode node : get("/api/nodes")) {
            if (node.path("running").asBoolean(false)) {
                running++;
            }
        }
        return running;
    }

    /**
     * The value once {@code settled} accepts it. A read that fails counts as not settled. When the deadline passes
     * first, fails with the last value. Polls every tenth of a second.
     */
    public static <T> T eventually(Duration deadline, Supplier<T> read, Predicate<T> settled) {
        long until = System.nanoTime() + deadline.toNanos();
        T value = null;
        RuntimeException failure = null;
        while (true) {
            try {
                value = read.get();
                failure = null;
                if (settled.test(value)) {
                    return value;
                }
            } catch (RuntimeException e) {
                failure = e;
            }
            if (System.nanoTime() >= until) {
                break;
            }
            LockSupport.parkNanos(POLL.toNanos());
            if (Thread.interrupted()) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting", failure);
            }
        }
        throw new IllegalStateException("Not settled within " + deadline + ", last value: " + value, failure);
    }
}
