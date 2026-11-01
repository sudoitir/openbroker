package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.CallInfo;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.Outcome;
import io.github.sudoitir.openbroker.Permit;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.TlsMaterial;
import io.github.sudoitir.openbroker.rabbitmq.RabbitMqOptions;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.MissingNode;

/**
 * The management API client: JSON through Jackson 3, preemptive authentication on every request, a permit from the
 * call interceptor, the request timeout set to the time left on the deadline, the body capped, and endpoint failover
 * after a request that was not sent. It never logs a body or a credential.
 */
public final class ManagementClient {

    /** A response: its JSON body ({@code MissingNode} when empty) and the {@code Location} header when present. */
    public record Response(JsonNode body, Optional<String> location) {}

    private final RabbitMqCluster cluster;

    ManagementClient(RabbitMqCluster cluster) {
        this.cluster = cluster;
    }

    public Response get(String path, Map<String, String> query, Deadline deadline, String operation) {
        return send("GET", path, query, null, deadline, operation);
    }

    public Response put(String path, JsonNode body, Deadline deadline, String operation) {
        return send("PUT", path, Map.of(), body, deadline, operation);
    }

    public Response post(String path, JsonNode body, Deadline deadline, String operation) {
        return send("POST", path, Map.of(), body, deadline, operation);
    }

    public Response delete(String path, Map<String, String> query, Deadline deadline, String operation) {
        return send("DELETE", path, query, null, deadline, operation);
    }

    private Response send(
            String method, String path, Map<String, String> query, JsonNode body, Deadline deadline, String operation) {
        ConnectOptions options = cluster.options();
        if (deadline.expired(options.clock())) {
            throw Deadlines.exceeded(RabbitMqCluster.CLUSTER, Dispatch.NOT_SENT);
        }
        List<RabbitMqCluster.Target> targets = cluster.managementTargets();
        if (targets.isEmpty()) {
            throw RabbitMqErrors.noEndpoint(
                    "Add a management endpoint (http or https): RabbitMQ is read and changed" + " through it.");
        }
        byte[] payload = body == null ? null : cluster.mapper().writeValueAsBytes(body);
        BrokerException last = null;
        for (int i = 0; i < targets.size(); i++) {
            RabbitMqCluster.Target target = targets.get(i);
            Permit permit = options.interceptor()
                    .admit(new CallInfo(
                            target.node(), target.base().toString(), Plane.MANAGEMENT, operation, 1, Optional.empty()));
            try (permit) {
                Exchanged done = exchange(method, target, path, query, payload, deadline, operation);
                permit.complete(Outcome.succeeded(), done.serverTime());
                return done.response();
            } catch (BrokerException e) {
                BrokerException located = RabbitMqErrors.at(e, target.node());
                permit.complete(Outcome.failed(located), Optional.empty());
                last = located;
                if (located.dispatch() != Dispatch.NOT_SENT || i == targets.size() - 1) {
                    throw located;
                }
            }
        }
        throw last;
    }

    private record Exchanged(Response response, Optional<Instant> serverTime) {}

    private Exchanged exchange(
            String method,
            RabbitMqCluster.Target target,
            String path,
            Map<String, String> query,
            byte[] payload,
            Deadline deadline,
            String operation) {
        ConnectOptions options = cluster.options();
        Duration timeout = deadline.remaining(options.clock());
        if (timeout.isZero()) {
            throw Deadlines.exceeded(target.node(), Dispatch.NOT_SENT);
        }
        HttpClient http = HttpClients.forTls(
                cluster.context(), tls(target, options), options.timeouts().connect());
        URI uri = URI.create(target.base() + path + queryString(query));
        HttpRequest.Builder request =
                HttpRequest.newBuilder(uri).timeout(timeout).header("Accept", "application/json");
        if (payload != null) {
            request.header("Content-Type", "application/json");
            request.method(method, HttpRequest.BodyPublishers.ofByteArray(payload));
        } else {
            request.method(method, HttpRequest.BodyPublishers.noBody());
        }
        authorize(request, cluster.credentials().forPlane(Plane.MANAGEMENT));
        boolean write = !"GET".equals(method);
        HttpResponse<InputStream> response;
        try {
            response = http.send(request.build(), HttpResponse.BodyHandlers.ofInputStream());
        } catch (IOException e) {
            throw RabbitMqErrors.fromIo(e, true);
        } catch (InterruptedException e) {
            throw Deadlines.cancelled(target.node(), e);
        }
        long max = options.maxResponseBytes();
        byte[] bytes;
        try (InputStream in = response.body()) {
            bytes = in.readNBytes((int) Math.min(Integer.MAX_VALUE - 8L, max + 1));
        } catch (IOException e) {
            throw RabbitMqErrors.fromIo(e, true);
        }
        if (bytes.length > max) {
            throw RabbitMqErrors.responseTooLarge(max);
        }
        int status = response.statusCode();
        if (status < 200 || status >= 300) {
            throw RabbitMqErrors.fromStatus(status, operation, path, reason(bytes), write);
        }
        return new Exchanged(
                new Response(parse(bytes), response.headers().firstValue("Location")),
                response.headers().firstValue("Date").flatMap(ManagementClient::date));
    }

    private static Optional<TlsMaterial> tls(RabbitMqCluster.Target target, ConnectOptions options) {
        Optional<TlsMaterial> material = options.tls().current();
        if (RabbitMqOptions.HTTPS.equals(target.protocol()) && material.isEmpty()) {
            throw RabbitMqErrors.invalid(
                    "A management endpoint uses https, and the connection has no TLS material: configure a TlsProvider.");
        }
        return material;
    }

    private static void authorize(HttpRequest.Builder request, Credentials credentials) {
        switch (credentials) {
            case Credentials.UsernamePassword account -> {
                String pair = account.username() + ":" + account.password().reveal();
                request.header(
                        "Authorization",
                        "Basic " + Base64.getEncoder().encodeToString(pair.getBytes(StandardCharsets.UTF_8)));
            }
            case Credentials.BearerToken token ->
                request.header("Authorization", "Bearer " + token.token().reveal());
            case Credentials.None none -> {
                // No header: the management API answers 401, which maps to a credentials failure.
            }
        }
    }

    private static String queryString(Map<String, String> query) {
        if (query.isEmpty()) {
            return "";
        }
        StringBuilder out = new StringBuilder("?");
        query.forEach((k, v) -> {
            if (out.length() > 1) {
                out.append('&');
            }
            out.append(ManagementPaths.segment(k)).append('=').append(ManagementPaths.segment(v));
        });
        return out.toString();
    }

    private JsonNode parse(byte[] bytes) {
        if (bytes.length == 0) {
            return MissingNode.getInstance();
        }
        try {
            return cluster.mapper().readTree(bytes);
        } catch (JacksonException e) {
            throw RabbitMqErrors.badResponse("RabbitMQ answered 200 with a body that is not JSON.", e);
        }
    }

    /** The {@code reason} of an error body, or empty when the body is not the management API's. */
    private String reason(byte[] bytes) {
        if (bytes.length == 0) {
            return "";
        }
        try {
            JsonNode node = cluster.mapper().readTree(bytes);
            JsonNode reason = node.path("reason");
            return reason.isString() ? reason.asString() : "";
        } catch (JacksonException e) {
            return "";
        }
    }

    private static Optional<Instant> date(String header) {
        try {
            return Optional.of(ZonedDateTime.parse(header, DateTimeFormatter.RFC_1123_DATE_TIME)
                    .toInstant());
        } catch (DateTimeParseException e) {
            return Optional.empty();
        }
    }
}
