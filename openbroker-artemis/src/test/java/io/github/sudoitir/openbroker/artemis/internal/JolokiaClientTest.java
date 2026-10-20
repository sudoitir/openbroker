package io.github.sudoitir.openbroker.artemis.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.assertj.core.api.Assertions.tuple;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.CallInfo;
import io.github.sudoitir.openbroker.CallInterceptor;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Permit;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.Timeouts;
import io.github.sudoitir.openbroker.artemis.JolokiaRequest;
import io.github.sudoitir.openbroker.artemis.JolokiaResponse;
import io.github.sudoitir.openbroker.artemis.internal.JolokiaClient.BrokerNames;
import java.io.IOException;
import java.net.Authenticator;
import java.net.ConnectException;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.IntStream;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class JolokiaClientTest {

    private static final NodeId NODE = NodeId.of("node-1");
    private static final String BROKER = "org.apache.activemq.artemis:broker=\"primary\"";
    private static final Clock CLOCK = Clock.systemUTC();

    private final FixtureServer server = new FixtureServer();
    private final BrokerNames names = new BrokerNames();
    private final List<CallInfo> admitted = new ArrayList<>();

    @AfterEach
    void stop() {
        server.close();
    }

    private JolokiaClient client() {
        return client(HttpClient.newHttpClient(), server.endpoint(), 1 << 20);
    }

    private JolokiaClient client(HttpClient http, URI endpoint, long maxResponseBytes) {
        CallInterceptor recording = call -> {
            admitted.add(call);
            return Permit.unlimited();
        };
        return new JolokiaClient(
                http,
                endpoint,
                NODE,
                () -> Credentials.of("admin", "s3cret"),
                JsonMapper.builder().build(),
                names,
                recording,
                CLOCK,
                Timeouts.fixed(Duration.ofSeconds(2), Duration.ofSeconds(5)),
                maxResponseBytes);
    }

    private static Deadline deadline() {
        return Deadline.after(Duration.ofSeconds(5), CLOCK);
    }

    private BrokerException failure(Runnable call) {
        return catchThrowableOfType(BrokerException.class, call::run);
    }

    @Test
    void aRequestIsOnePostWithPreemptiveBasicAuthAndJson() {
        server.json("search-broker.json");

        client().search("org.apache.activemq.artemis:broker=*", deadline());

        FixtureServer.Received sent = server.received().getFirst();
        assertThat(sent.method()).isEqualTo("POST");
        assertThat(sent.headers().get("Authorization"))
                .containsExactly("Basic " + Base64.getEncoder().encodeToString("admin:s3cret".getBytes()));
        assertThat(sent.headers().get("Content-type")).containsExactly("application/json");
        assertThat(sent.headers().get("Accept")).containsExactly("application/json");
        assertThat(sent.body()).contains("\"type\":\"search\"");
    }

    @Test
    void everyRequestIsAdmittedAndABatchCostsOnePermitPerFiftyOperations() {
        server.json("search-broker.json").respond(200, Map.of("Content-Type", "application/json"), "[]");
        JolokiaClient client = client();

        client.single(JolokiaRequest.search("org.apache.activemq.artemis:*"), deadline(), Optional.empty());
        client.batch(
                IntStream.range(0, 120)
                        .mapToObj(i -> JolokiaRequest.read("x", "MessageCount"))
                        .toList(),
                deadline(),
                Optional.empty());

        assertThat(admitted)
                .extracting(CallInfo::operation, CallInfo::cost, CallInfo::plane)
                .containsExactly(tuple("single", 1, Plane.MANAGEMENT), tuple("batch", 3, Plane.MANAGEMENT));
        assertThat(admitted).allSatisfy(call -> assertThat(call.node()).isEqualTo(NODE));
    }

    @Test
    void resolvesTheBrokerNameOnceAndSharesIt() {
        server.json("search-broker.json");

        assertThat(client().brokerName(deadline())).isEqualTo(BROKER);
        assertThat(client().brokerName(deadline())).isEqualTo(BROKER);
        assertThat(server.received()).hasSize(1);
    }

    @Test
    void anEmptySearchMeansThisIsNotArtemis() {
        server.json("search-broker-empty.json");

        BrokerException e = failure(() -> client().brokerName(deadline()));

        assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
        assertThat(e.reason()).isEqualTo(ErrorReasons.UNEXPECTED_SYSTEM);
    }

    @Test
    void aDoubleEncodedTopologyIsParsedASecondTime() {
        server.json("topology.json");
        JolokiaClient client = client();

        JolokiaResponse response =
                client.single(JolokiaRequest.exec("x", "listNetworkTopology()"), deadline(), Optional.empty());
        assertThat(response.value().isString()).isTrue();

        JsonNode parsed = client.parsed(response);
        assertThat(parsed.get(0).get("nodeID").asString()).isEqualTo("f7734597-a768-11f1-aa4c-ceae3fa2df1d");
        assertThat(parsed.get(0).get("live").asString()).isEqualTo("artemis-primary:61616");
        assertThat(parsed.get(0).get("backup").asString()).isEqualTo("artemis-backup:61616");
    }

    @Test
    void aTopologyAfterFailoverHasNoBackup() {
        server.json("topology-after-failover.json");
        JolokiaClient client = client();

        JsonNode parsed = client.parsed(
                client.single(JolokiaRequest.exec("x", "listNetworkTopology()"), deadline(), Optional.empty()));

        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).has("backup")).isFalse();
        assertThat(parsed.get(0).get("live").asString()).isEqualTo("artemis-backup:61616");
    }

    @Test
    void aBatchKeepsAFailedEntryApartFromTheOthers() {
        server.json("batch-mixed-status.json");

        List<JolokiaResponse> responses = client().batch(
                        List.of(
                                JolokiaRequest.read("b", "Active", "ReplicaSync", "NodeID"),
                                JolokiaRequest.exec("b", "listNetworkTopology()"),
                                JolokiaRequest.exec("b", "listQueues(java.lang.String,int,int)", "", 1, 50),
                                JolokiaRequest.read("bad", "X")),
                        deadline(),
                        Optional.empty());

        assertThat(responses).hasSize(4);
        assertThat(responses.subList(0, 3)).allSatisfy(r -> assertThat(r.ok()).isTrue());
        assertThat(responses.get(0).value().get("Active").asBoolean()).isTrue();
        JolokiaResponse failed = responses.get(3);
        assertThat(failed.ok()).isFalse();
        assertThat(failed.status()).isEqualTo(404);
        assertThat(failed.errorType()).isEqualTo("javax.management.InstanceNotFoundException");
    }

    @Test
    void aMultiAttributeReadIsAnObject() {
        server.json("ha-read-primary.json");

        JsonNode value = client().single(
                        JolokiaRequest.read("b", "Active", "Started", "Backup", "ReplicaSync", "NodeID"),
                        deadline(),
                        Optional.empty())
                .value();

        assertThat(value.get("Active").asBoolean()).isTrue();
        assertThat(value.get("Backup").asBoolean()).isFalse();
        assertThat(value.get("NodeID").asString()).isEqualTo("f7734597-a768-11f1-aa4c-ceae3fa2df1d");
    }

    @Test
    void unauthorizedIsRejectedCredentialsOnTheManagementPlane() {
        server.respond(401, Map.of("WWW-Authenticate", "Bearer realm=\"artemis\""), "");

        BrokerException e = failure(() -> client().search("x:*", deadline()));

        assertThat(e.status()).isEqualTo(Status.UNAUTHENTICATED);
        assertThat(e.reason()).isEqualTo(ErrorReasons.CREDENTIALS_REJECTED);
        assertThat(e.info().metadata()).containsEntry(ErrorReasons.PLANE, Plane.MANAGEMENT.name());
        assertThat(e.dispatch()).isEqualTo(Dispatch.REJECTED);
        assertThat(e.getMessage()).contains("Bearer realm=\"artemis\"");
    }

    @Test
    void aBareHawtioForbiddenIsRejectedCredentialsWithTheReasonExplained() {
        server.respond(403, Map.of("Hawtio-Forbidden-Reason", "NONE"), "");

        BrokerException e = failure(() -> client().search("x:*", deadline()));

        assertThat(e.status()).isEqualTo(Status.PERMISSION_DENIED);
        assertThat(e.reason()).isEqualTo(ErrorReasons.CREDENTIALS_REJECTED);
        assertThat(e.getMessage()).contains("Hawtio-Forbidden-Reason: NONE").contains("no authenticated session");
    }

    @Test
    void notFoundIsTheWrongPath() {
        server.respond(404, Map.of(), "");

        BrokerException e = failure(() -> client().search("x:*", deadline()));

        assertThat(e.reason()).isEqualTo(ErrorReasons.ENDPOINT_NOT_FOUND);
    }

    @Test
    void aRedirectIsNeverFollowedAndNamesWhereItPointed() {
        server.respond(302, Map.of("Location", "/console/auth/login"), "");

        BrokerException e = failure(() -> client().search("x:*", deadline()));

        assertThat(e.reason()).isEqualTo(ErrorReasons.ENDPOINT_NOT_FOUND);
        assertThat(e.getMessage()).contains("/console/auth/login");
        assertThat(server.received()).hasSize(1);
    }

    @Test
    void anHtmlPageOnTwoHundredNamesTheConsole() {
        server.respond(200, Map.of("Content-Type", "text/html"), "<!DOCTYPE html><html><body>Login</body></html>");

        BrokerException e = failure(() -> client().search("x:*", deadline()));

        assertThat(e.reason()).isEqualTo(ErrorReasons.BAD_RESPONSE);
        assertThat(e.getMessage()).contains("/console/jolokia");
    }

    @Test
    void jolokiaJsonLabelledTextPlainIsStillParsed() {
        // Artemis 2.39's bundled agent labels its JSON text/plain.
        server.respond(
                200, Map.of("Content-Type", "text/plain;charset=utf-8"), FixtureServer.fixture("search-broker.json"));

        assertThat(client().brokerName(deadline())).isEqualTo(BROKER);
    }

    @Test
    void anAnswerOverTheLimitIsRefusedRatherThanRead() {
        server.json("topology.json");

        BrokerException e = failure(() -> client(HttpClient.newHttpClient(), server.endpoint(), 16)
                .single(JolokiaRequest.exec("x", "listNetworkTopology()"), deadline(), Optional.empty()));

        assertThat(e.status()).isEqualTo(Status.RESOURCE_EXHAUSTED);
    }

    @Test
    void anExpiredDeadlineSendsNothing() {
        BrokerException e = failure(() ->
                client().single(JolokiaRequest.search("x:*"), Deadline.after(Duration.ZERO, CLOCK), Optional.empty()));

        assertThat(e.status()).isEqualTo(Status.DEADLINE_EXCEEDED);
        assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
        assertThat(server.received()).isEmpty();
    }

    @Test
    void aBrokerThatReportsItsMBeanMissingIsResolvedAgain() {
        server.json("search-broker.json")
                .json("ha-read-instance-not-found.json")
                .json("search-broker.json");
        JolokiaClient client = client();

        assertThat(client.readBrokerAttributes(deadline(), "Active").ok()).isFalse();
        assertThat(names.get(server.endpoint())).isNull();

        assertThat(client.brokerName(deadline())).isEqualTo(BROKER);
        assertThat(server.received()).hasSize(3);
    }

    @Test
    void aFailureOfAnotherMBeanKeepsTheBrokerName() {
        server.json("search-broker.json").json("ha-read-instance-not-found.json");
        JolokiaClient client = client();
        client.brokerName(deadline());

        client.single(
                JolokiaRequest.read("org.apache.activemq.artemis:queue=\"gone\"", "MessageCount"),
                deadline(),
                Optional.empty());

        assertThat(names.get(server.endpoint())).isEqualTo(BROKER);
    }

    private BrokerException transportFailure(IOException cause) {
        URI endpoint = URI.create("http://broker-1:8161/console/jolokia");
        names.put(endpoint, BROKER);
        BrokerException e = failure(() -> client(new FailingHttpClient(cause), endpoint, 1 << 20)
                .single(JolokiaRequest.search("x:*"), deadline(), Optional.empty()));
        assertThat(names.get(endpoint))
                .as("a transport failure forgets the broker name")
                .isNull();
        assertThat(e.status()).isEqualTo(Status.UNAVAILABLE);
        return e;
    }

    @Test
    void aReadTimeoutSaysTheBrokerAcceptedTheConnectionButDidNotAnswer() {
        BrokerException e = transportFailure(new HttpTimeoutException("request timed out"));

        assertThat(e.getMessage()).contains("broker-1:8161").contains("did not answer within the read timeout");
        assertThat(e.dispatch()).isEqualTo(Dispatch.IN_DOUBT);
    }

    @Test
    void aConnectTimeoutSaysNothingAcceptedTheConnection() {
        BrokerException e = transportFailure(new HttpConnectTimeoutException("HTTP connect timed out"));

        assertThat(e.getMessage()).contains("broker-1:8161").contains("did not accept the connection");
        assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
    }

    @Test
    void aRefusedConnectionSaysNothingIsListening() {
        BrokerException e = transportFailure(new ConnectException("Connection refused"));

        assertThat(e.getMessage()).contains("nothing is listening at broker-1:8161");
        assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
    }

    @Test
    void anUnresolvableHostSaysTheNameDoesNotResolve() {
        BrokerException e = transportFailure(new UnknownHostException("broker-1"));

        assertThat(e.getMessage()).contains("broker-1:8161").contains("does not resolve");
    }

    @Test
    void anyOtherTransportFailureKeepsTheRootCause() {
        BrokerException e = transportFailure(new IOException("HTTP/1.1 header parser received no bytes"));

        assertThat(e.getMessage()).contains("broker-1:8161").contains("header parser received no bytes");
        assertThat(e.dispatch()).isEqualTo(Dispatch.IN_DOUBT);
    }

    @Test
    void credentialsNeverReachTheMessage() {
        server.respond(401, Map.of(), "");

        assertThatThrownBy(() -> client().search("x:*", deadline()))
                .isInstanceOf(BrokerException.class)
                .satisfies(e -> assertThat(e.toString()).doesNotContain("s3cret"));
    }

    /** An HTTP client whose every send fails with one I/O error. */
    private static final class FailingHttpClient extends HttpClient {

        private final IOException failure;

        FailingHttpClient(IOException failure) {
            this.failure = failure;
        }

        @Override
        public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> handler) throws IOException {
            throw failure;
        }

        @Override
        public <T> CompletableFuture<HttpResponse<T>> sendAsync(
                HttpRequest request, HttpResponse.BodyHandler<T> handler) {
            return CompletableFuture.failedFuture(failure);
        }

        @Override
        public <T> CompletableFuture<HttpResponse<T>> sendAsync(
                HttpRequest request, HttpResponse.BodyHandler<T> handler, HttpResponse.PushPromiseHandler<T> push) {
            return CompletableFuture.failedFuture(failure);
        }

        @Override
        public Optional<CookieHandler> cookieHandler() {
            return Optional.empty();
        }

        @Override
        public Optional<Duration> connectTimeout() {
            return Optional.empty();
        }

        @Override
        public Redirect followRedirects() {
            return Redirect.NEVER;
        }

        @Override
        public Optional<ProxySelector> proxy() {
            return Optional.empty();
        }

        @Override
        public SSLContext sslContext() {
            return null;
        }

        @Override
        public SSLParameters sslParameters() {
            return new SSLParameters();
        }

        @Override
        public Optional<Authenticator> authenticator() {
            return Optional.empty();
        }

        @Override
        public Version version() {
            return Version.HTTP_1_1;
        }

        @Override
        public Optional<Executor> executor() {
            return Optional.empty();
        }
    }
}
