package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.CallInfo;
import io.github.sudoitir.openbroker.CallInterceptor;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Outcome;
import io.github.sudoitir.openbroker.Permit;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Timeouts;
import io.github.sudoitir.openbroker.artemis.ArtemisMBeans;
import io.github.sudoitir.openbroker.artemis.JolokiaRequest;
import io.github.sudoitir.openbroker.artemis.JolokiaResponse;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import javax.net.ssl.SSLException;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * A Jolokia client bound to one node's management endpoint. Every call is one HTTP POST; {@link
 * #batch} sends a JSON array, so a read of many attributes costs one request per node. Each request
 * asks the caller's {@link CallInterceptor} for a permit first, at one permit per request and one per
 * {@value #OPERATIONS_PER_PERMIT} operations in a batch, so batching never exceeds a rate limit.
 */
public final class JolokiaClient {

    /** Operations one permit covers in a batch. */
    public static final int OPERATIONS_PER_PERMIT = 50;

    /** Hawtio names its own refusal reason here; Jolokia itself never sets it. */
    private static final String HAWTIO_FORBIDDEN_REASON = "Hawtio-Forbidden-Reason";

    private final HttpClient http;
    private final URI endpoint;
    private final NodeId node;
    private final Supplier<Credentials> credentials;
    private final ObjectMapper mapper;
    private final BrokerNames brokerNames;
    private final CallInterceptor interceptor;
    private final Clock clock;
    private final Timeouts timeouts;
    private final long maxResponseBytes;

    public JolokiaClient(
            HttpClient http,
            URI endpoint,
            NodeId node,
            Supplier<Credentials> credentials,
            ObjectMapper mapper,
            BrokerNames brokerNames,
            CallInterceptor interceptor,
            Clock clock,
            Timeouts timeouts,
            long maxResponseBytes) {
        this.http = http;
        this.endpoint = endpoint;
        this.node = node;
        this.credentials = credentials;
        this.mapper = mapper;
        this.brokerNames = brokerNames;
        this.interceptor = interceptor;
        this.clock = clock;
        this.timeouts = timeouts;
        this.maxResponseBytes = maxResponseBytes;
    }

    public URI endpoint() {
        return endpoint;
    }

    public NodeId node() {
        return node;
    }

    public ObjectMapper mapper() {
        return mapper;
    }

    /** Sends a batch; the result is aligned with {@code requests}. */
    public List<JolokiaResponse> batch(List<JolokiaRequest> requests, Deadline deadline, Optional<ResourceId> target) {
        JsonNode body = post(requests, permits(requests.size()), "batch", deadline, target, true);
        List<JolokiaResponse> entries = new ArrayList<>();
        if (body != null && body.isArray()) {
            body.forEach(e -> entries.add(toResponse(e)));
        }
        return entries;
    }

    public JolokiaResponse single(JolokiaRequest request, Deadline deadline, Optional<ResourceId> target) {
        JsonNode body = post(request, 1, "single", deadline, target, false);
        if (body == null || !body.isObject()) {
            throw Errors.badResponse(node, Errors.BAD_RESPONSE_MESSAGE, null);
        }
        return toResponse(body);
    }

    /** The value of one batch entry, parsed again when it is a JSON string; throws if the entry failed. */
    public @Nullable JsonNode parsed(JolokiaResponse entry) {
        if (!entry.ok()) {
            throw Errors.badResponse(node, "Batch entry failed: " + entry.failure(), null);
        }
        return entry.valueParsed(mapper);
    }

    /** MBean names matching a JMX pattern. */
    public List<String> search(String pattern, Deadline deadline) {
        JolokiaResponse response = single(JolokiaRequest.search(pattern), deadline, Optional.empty());
        if (!response.ok() || response.value() == null || !response.value().isArray()) {
            return List.of();
        }
        List<String> names = new ArrayList<>();
        response.value().forEach(n -> names.add(n.asString()));
        return names;
    }

    /**
     * The broker's top-level MBean name, resolved once with a {@code search} and shared across clients
     * of the same endpoint.
     */
    public String brokerName(Deadline deadline) {
        String cached = brokerNames.get(endpoint);
        if (cached != null) {
            return cached;
        }
        List<String> matches = search(ArtemisMBeans.BROKER_SEARCH_PATTERN, deadline);
        if (matches.isEmpty()) {
            throw Errors.notArtemis(node);
        }
        String name = matches.getFirst();
        brokerNames.put(endpoint, name);
        return name;
    }

    /** A call on the broker MBean; a broker that says the MBean is gone is resolved again next time. */
    public JolokiaResponse onBroker(JolokiaRequest request, Deadline deadline, Optional<ResourceId> target) {
        JolokiaResponse response = single(request, deadline, target);
        if (!response.ok()
                && response.errorType() != null
                && response.errorType().contains(Errors.MBEAN_ABSENT)) {
            brokerNames.forget(endpoint);
        }
        return response;
    }

    public JolokiaResponse readBrokerAttributes(Deadline deadline, String... attributes) {
        return onBroker(JolokiaRequest.read(brokerName(deadline), attributes), deadline, Optional.empty());
    }

    public JolokiaResponse execOnBroker(
            Deadline deadline, Optional<ResourceId> target, String operation, Object... arguments) {
        return onBroker(JolokiaRequest.exec(brokerName(deadline), operation, arguments), deadline, target);
    }

    /** A broker operation whose value is a JSON string; returns the parsed node. */
    public @Nullable JsonNode execOnBrokerParsed(Deadline deadline, String operation, Object... arguments) {
        JolokiaResponse response = execOnBroker(deadline, Optional.empty(), operation, arguments);
        if (!response.ok()) {
            throw Errors.badResponse(node, "Operation " + operation + " failed: " + response.failure(), null);
        }
        return response.valueParsed(mapper);
    }

    private static int permits(int operations) {
        return Math.max(1, (operations + OPERATIONS_PER_PERMIT - 1) / OPERATIONS_PER_PERMIT);
    }

    private JolokiaResponse toResponse(JsonNode node) {
        return mapper.treeToValue(node, JolokiaResponse.class);
    }

    private @Nullable JsonNode post(
            Object payload, int cost, String operation, Deadline deadline, Optional<ResourceId> target, boolean batch) {
        if (deadline.expired(clock)) {
            throw Deadlines.exceeded(node, Dispatch.NOT_SENT);
        }
        byte[] body = mapper.writeValueAsBytes(payload);
        try (Permit permit =
                interceptor.admit(new CallInfo(node, endpoint.toString(), Plane.MANAGEMENT, operation, cost, target))) {
            try {
                JsonNode answer = exchange(body, deadline);
                permit.complete(Outcome.succeeded(), serverTime(answer, batch));
                return answer;
            } catch (BrokerException e) {
                permit.complete(Outcome.failed(e), Optional.empty());
                throw e;
            }
        }
    }

    /** The broker's clock from the first successful entry, in epoch seconds. */
    private static Optional<Instant> serverTime(@Nullable JsonNode answer, boolean batch) {
        JsonNode entry = null;
        if (answer != null && batch && answer.isArray()) {
            for (JsonNode e : answer) {
                if (e.path("status").asInt() == 200 && !e.has("error")) {
                    entry = e;
                    break;
                }
            }
        } else if (answer != null && answer.path("status").asInt() == 200 && !answer.has("error")) {
            entry = answer;
        }
        if (entry == null || !entry.hasNonNull("timestamp")) {
            return Optional.empty();
        }
        return Optional.of(Instant.ofEpochSecond(entry.get("timestamp").asLong()));
    }

    private @Nullable JsonNode exchange(byte[] body, Deadline deadline) {
        Duration timeout = deadline.remaining(clock);
        Duration readTimeout = timeouts.call();
        if (timeout.compareTo(readTimeout) > 0) {
            timeout = readTimeout;
        }
        if (timeout.isZero()) {
            throw Deadlines.exceeded(node, Dispatch.NOT_SENT);
        }
        HttpRequest.Builder request = HttpRequest.newBuilder(endpoint)
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        Credentials account = credentials.get();
        if (!account.isNone()) {
            String pair = account.username() + ":" + account.password().reveal();
            request.header(
                    "Authorization",
                    "Basic " + Base64.getEncoder().encodeToString(pair.getBytes(StandardCharsets.UTF_8)));
        }
        HttpResponse<InputStream> response;
        try {
            response = http.send(request.build(), HttpResponse.BodyHandlers.ofInputStream());
        } catch (IOException e) {
            // The broker may have restarted or been replaced at this address; learn its name again.
            brokerNames.forget(endpoint);
            throw transportFailure(e);
        } catch (InterruptedException e) {
            throw Deadlines.cancelled(node, e);
        }
        return read(response);
    }

    private @Nullable JsonNode read(HttpResponse<InputStream> response) {
        int status = response.statusCode();
        byte[] bytes;
        try (InputStream in = response.body()) {
            bytes = in.readNBytes((int) Math.min(Integer.MAX_VALUE - 8, maxResponseBytes + 1));
        } catch (IOException e) {
            brokerNames.forget(endpoint);
            throw Errors.unreachable(
                    node,
                    "Could not reach the broker at " + NodeAddress.hostPort(endpoint) + ": " + describe(e),
                    Dispatch.IN_DOUBT,
                    e);
        }
        if (bytes.length > maxResponseBytes) {
            throw Errors.responseTooLarge(node, maxResponseBytes);
        }
        if (status >= 300 && status < 400) {
            String location = response.headers().firstValue("Location").orElse(null);
            throw Errors.wrongPath(
                    node,
                    "The broker redirected the request"
                            + (location != null && !location.isBlank() ? " to " + location : "")
                            + " instead of answering it. That is the console UI, not the Jolokia agent"
                            + " — the URL should end in /console/jolokia.");
        }
        if (status == 401 || status == 403) {
            throw Errors.credentialsRejected(
                    node, Plane.MANAGEMENT, status, Errors.CREDENTIALS_MESSAGE + refusalHint(response));
        }
        if (status == 404) {
            throw Errors.wrongPath(node, Errors.WRONG_PATH_MESSAGE);
        }
        if (status < 200 || status >= 300) {
            throw Errors.badResponse(node, "The broker responded " + status + ".", null);
        }
        String type = response.headers().firstValue("Content-Type").orElse("").toLowerCase();
        if (!type.isEmpty() && !type.contains("json") && !type.startsWith("text/plain")) {
            String hint = type.startsWith("text/html")
                    ? " It answered with an HTML page, which is the console UI rather than the"
                            + " Jolokia agent — check that the URL ends in /console/jolokia."
                    : " The response content type was " + type + ", not JSON.";
            throw Errors.badResponse(node, Errors.BAD_RESPONSE_MESSAGE + hint, null);
        }
        if (bytes.length == 0) {
            return null;
        }
        try {
            return mapper.readTree(bytes);
        } catch (JacksonException e) {
            throw Errors.badResponse(node, Errors.BAD_RESPONSE_MESSAGE, e);
        }
    }

    /** Says which way the transport failed, so a slow broker does not read the same as a missing one. */
    private BrokerException transportFailure(IOException e) {
        String at = NodeAddress.hostPort(endpoint);
        if (hasCause(e, SSLException.class)) {
            return Errors.tls(node, Errors.TLS_MESSAGE, e);
        }
        if (hasCause(e, HttpConnectTimeoutException.class)) {
            return Errors.unreachable(
                    node,
                    "The broker at " + at + " did not accept the connection within the connect timeout.",
                    Dispatch.NOT_SENT,
                    e);
        }
        if (hasCause(e, HttpTimeoutException.class)) {
            return Errors.unreachable(
                    node,
                    "The broker at " + at + " accepted the connection but did not answer within the read timeout.",
                    Dispatch.IN_DOUBT,
                    e);
        }
        if (hasCause(e, ConnectException.class)) {
            return Errors.unreachable(
                    node, "Connection refused: nothing is listening at " + at + ".", Dispatch.NOT_SENT, e);
        }
        if (hasCause(e, UnknownHostException.class)) {
            return Errors.unreachable(node, "The host name of " + at + " does not resolve.", Dispatch.NOT_SENT, e);
        }
        return Errors.unreachable(
                node, "Could not reach the broker at " + at + ": " + describe(e), Dispatch.IN_DOUBT, e);
    }

    private static String describe(Throwable e) {
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getMessage() != null ? root.getMessage() : root.getClass().getSimpleName();
    }

    /** Turns the broker's refusal headers into something an operator can act on. */
    private static String refusalHint(HttpResponse<?> response) {
        StringBuilder hint = new StringBuilder();
        String hawtio = header(response, HAWTIO_FORBIDDEN_REASON);
        if (hawtio != null) {
            hint.append(" The console's Hawtio filter refused it (")
                    .append(HAWTIO_FORBIDDEN_REASON)
                    .append(": ")
                    .append(hawtio)
                    .append(")");
            hint.append(
                    "NONE".equalsIgnoreCase(hawtio)
                            ? " — that reason means no authenticated session, so the username or password is"
                                    + " missing or wrong, or the account holds no role allowed on the console."
                            : ".");
        }
        String challenge = header(response, "WWW-Authenticate");
        if (challenge != null) {
            hint.append(" The broker asked for: ").append(challenge).append(".");
        }
        return hint.toString();
    }

    private static @Nullable String header(HttpResponse<?> response, String name) {
        return response.headers()
                .firstValue(name)
                .map(String::trim)
                .filter(v -> !v.isBlank())
                .orElse(null);
    }

    private static boolean hasCause(Throwable t, Class<? extends Throwable> type) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (type.isInstance(c)) {
                return true;
            }
        }
        return false;
    }

    /** Broker MBean names resolved per endpoint, shared across clients. */
    public static final class BrokerNames {
        private final Map<URI, String> names = new ConcurrentHashMap<>();

        @Nullable String get(URI endpoint) {
            return names.get(endpoint);
        }

        void put(URI endpoint, String name) {
            names.put(endpoint, name);
        }

        public void forget(URI endpoint) {
            names.remove(endpoint);
        }
    }
}
