package io.github.sudoitir.openbroker.rabbitmq.admin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.CallInfo;
import io.github.sudoitir.openbroker.CallInterceptor;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Outcome;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.MissingNode;

class ManagementClientTest {

    private static final String SECRET = "s3cret-marker";
    private static final String JSON = "Content-Type";

    private FakeManagement server;

    @BeforeEach
    void start() {
        server = new FakeManagement();
    }

    @AfterEach
    void stop() {
        server.close();
    }

    private static Deadline deadline() {
        return Deadline.after(Duration.ofSeconds(10), Clock.systemUTC());
    }

    private RabbitMqCluster cluster() {
        return FakeManagement.cluster(
                Credentials.usernamePassword("ops", SECRET), ConnectOptions.defaults(), server.endpoint());
    }

    private static BrokerException failure(Runnable call) {
        try {
            call.run();
        } catch (BrokerException e) {
            return e;
        }
        throw new AssertionError("expected a BrokerException");
    }

    @Test
    void getsJsonWithPreemptiveBasicAuth() {
        server.json("GET", "/api/overview", "overview.json");
        ManagementClient.Response response = cluster().management().get("/api/overview", Map.of(), deadline(), "read");
        assertThat(response.body().path("rabbitmq_version").asString()).isEqualTo("3.13.7");
        String expected = "Basic " + Base64.getEncoder().encodeToString(("ops:" + SECRET).getBytes());
        assertThat(server.received().getFirst().header("Authorization")).isEqualTo(expected);
        assertThat(server.received().getFirst().header("Accept")).isEqualTo("application/json, */*;q=0.1");
    }

    @Test
    void sendsABearerTokenAndNothingForNoCredentials() {
        server.json("GET", "/api/overview", "overview.json");
        RabbitMqCluster bearer = FakeManagement.cluster(
                Credentials.bearerToken("tok-1", Instant.now().plusSeconds(60)),
                ConnectOptions.defaults(),
                server.endpoint());
        bearer.management().get("/api/overview", Map.of(), deadline(), "read");
        RabbitMqCluster none =
                FakeManagement.cluster(new Credentials.None(), ConnectOptions.defaults(), server.endpoint());
        none.management().get("/api/overview", Map.of(), deadline(), "read");
        assertThat(server.received().get(0).header("Authorization")).isEqualTo("Bearer tok-1");
        assertThat(server.received().get(1).header("Authorization")).isNull();
    }

    @Test
    void encodesTheQueryAndSendsBodiesWithTheirMethod() {
        JsonMapper mapper = JsonMapper.builder().build();
        server.respond("GET", "/api/queues/%2F", 200, Map.of(JSON, "application/json"), "[]")
                .respond("PUT", "/api/queues/%2F/q", 201, Map.of(), "")
                .respond("POST", "/api/purge", 200, Map.of(), "{}")
                .respond("DELETE", "/api/queues/%2F/q", 204, Map.of("Location", "/x"), "");
        ManagementClient client = cluster().management();
        client.get("/api/queues/%2F", Map.of("name", "a b&c"), deadline(), "list");
        ManagementClient.Response created =
                client.put("/api/queues/%2F/q", mapper.createObjectNode().put("durable", true), deadline(), "create");
        client.post("/api/purge", mapper.createObjectNode().put("a", 1), deadline(), "post");
        ManagementClient.Response deleted = client.delete("/api/queues/%2F/q", Map.of(), deadline(), "delete");
        assertThat(server.received().get(0).query()).isEqualTo("name=a%20b%26c");
        assertThat(server.received().get(1).body()).isEqualTo("{\"durable\":true}");
        assertThat(server.received().get(1).header("Content-Type")).isEqualTo("application/json");
        assertThat(created.body()).isEqualTo(MissingNode.getInstance());
        assertThat(deleted.location()).contains("/x");
        assertThat(server.received())
                .extracting(FakeManagement.Received::method)
                .containsExactly("GET", "PUT", "POST", "DELETE");
    }

    @Test
    void keepsAPathPrefix() {
        server.json("GET", "/rmq/api/overview", "overview.json");
        Endpoint prefixed = Endpoint.of(Plane.MANAGEMENT, "http", server.url() + "/rmq/");
        FakeManagement.cluster(prefixed).management().get("/api/overview", Map.of(), deadline(), "read");
        assertThat(server.received().getFirst().path()).isEqualTo("/rmq/api/overview");
    }

    @Test
    void refusesARedirectWithoutFollowingIt() {
        server.respond("GET", "/api/overview", 302, Map.of("Location", server.url() + "/elsewhere"), "");
        BrokerException e = failure(() -> cluster().management().get("/api/overview", Map.of(), deadline(), "read"));
        assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
        assertThat(e.reason()).isEqualTo(ErrorReasons.ENDPOINT_NOT_FOUND);
        assertThat(server.received()).hasSize(1);
    }

    @Test
    void mapsHttpFailures() {
        server.respond("GET", "/login", 401, Map.of(), FakeManagement.fixture("error-login.json"));
        server.respond("GET", "/admin", 401, Map.of(), FakeManagement.fixture("error-not-admin.json"));
        server.respond("GET", "/forbidden", 403, Map.of(), "{}");
        server.respond("GET", "/gone", 404, Map.of(), "{\"error\":\"Object Not Found\",\"reason\":\"Not Found\"}");
        server.respond("GET", "/api/overview", 404, Map.of(), "");
        server.respond("PUT", "/put", 405, Map.of(), "");
        server.respond("PUT", "/conflict", 409, Map.of(), "");
        server.respond("PUT", "/exists", 400, Map.of(), FakeManagement.fixture("error-bad-request.json"));
        server.respond("GET", "/boom", 500, Map.of(), "");
        server.respond("PUT", "/boom", 500, Map.of(), "");
        server.respond("GET", "/slow", 503, Map.of(), "");
        ManagementClient c = cluster().management();
        JsonMapper mapper = JsonMapper.builder().build();
        var body = mapper.createObjectNode();

        BrokerException login = failure(() -> c.get("/login", Map.of(), deadline(), "login"));
        assertThat(login.status()).isEqualTo(Status.UNAUTHENTICATED);
        assertThat(login.reason()).isEqualTo(ErrorReasons.CREDENTIALS_REJECTED);
        assertThat(login.info().metadata()).containsEntry(ErrorReasons.PLANE, "MANAGEMENT");
        assertThat(login.dispatch()).isEqualTo(Dispatch.REJECTED);

        assertThat(failure(() -> c.get("/admin", Map.of(), deadline(), "x")).status())
                .isEqualTo(Status.PERMISSION_DENIED);
        assertThat(failure(() -> c.get("/forbidden", Map.of(), deadline(), "x")).reason())
                .isEqualTo(ErrorReasons.NOT_AUTHORIZED);
        assertThat(failure(() -> c.get("/gone", Map.of(), deadline(), "x")).reason())
                .isEqualTo(ErrorReasons.RESOURCE_GONE);
        assertThat(failure(() -> c.get("/api/overview", Map.of(), deadline(), "x"))
                        .reason())
                .isEqualTo(ErrorReasons.ENDPOINT_NOT_FOUND);
        assertThat(failure(() -> c.put("/put", body, deadline(), "x")).status()).isEqualTo(Status.UNIMPLEMENTED);
        BrokerException conflict = failure(() -> c.put("/conflict", body, deadline(), "x"));
        assertThat(conflict.status()).isEqualTo(Status.FAILED_PRECONDITION);
        assertThat(failure(() -> c.put("/exists", body, deadline(), "x")).reason())
                .isEqualTo(ErrorReasons.RESOURCE_EXISTS);
        assertThat(failure(() -> c.get("/boom", Map.of(), deadline(), "x")).dispatch())
                .isEqualTo(Dispatch.REJECTED);
        assertThat(failure(() -> c.put("/boom", body, deadline(), "x")).dispatch())
                .isEqualTo(Dispatch.IN_DOUBT);
        assertThat(failure(() -> c.get("/slow", Map.of(), deadline(), "x")).status())
                .isEqualTo(Status.UNAVAILABLE);
    }

    @Test
    void capsTheBody() {
        server.respond("GET", "/big", 200, Map.of(), "[" + "1,".repeat(200) + "1]");
        RabbitMqCluster small = FakeManagement.cluster(
                Credentials.usernamePassword("ops", SECRET),
                ConnectOptions.defaults().withMaxResponseBytes(64),
                server.endpoint());
        BrokerException e = failure(() -> small.management().get("/big", Map.of(), deadline(), "x"));
        assertThat(e.status()).isEqualTo(Status.RESOURCE_EXHAUSTED);
        assertThat(e.reason()).isEqualTo(ErrorReasons.RESPONSE_TOO_LARGE);
        assertThat(e.dispatch()).isEqualTo(Dispatch.IN_DOUBT);
    }

    @Test
    void aBodyThatIsNotJsonIsABadResponse() {
        server.respond("GET", "/html", 200, Map.of(), "<html>hello</html>");
        BrokerException e = failure(() -> cluster().management().get("/html", Map.of(), deadline(), "x"));
        assertThat(e.reason()).isEqualTo(ErrorReasons.BAD_RESPONSE);
        assertThat(e.dispatch()).isEqualTo(Dispatch.IN_DOUBT);
    }

    @Test
    void aSlowAnswerExceedsTheDeadlineAfterTheRequestWasSent() {
        server.hang("GET", "/slow");
        Deadline soon = Deadline.after(Duration.ofMillis(300), Clock.systemUTC());
        BrokerException e = failure(() -> cluster().management().get("/slow", Map.of(), soon, "x"));
        assertThat(e.status()).isEqualTo(Status.DEADLINE_EXCEEDED);
        assertThat(e.dispatch()).isEqualTo(Dispatch.IN_DOUBT);
    }

    @Test
    void anExpiredDeadlineSendsNothing() {
        Deadline passed = Deadline.after(Duration.ofSeconds(-1), Clock.systemUTC());
        BrokerException e = failure(() -> cluster().management().get("/api/overview", Map.of(), passed, "x"));
        assertThat(e.status()).isEqualTo(Status.DEADLINE_EXCEEDED);
        assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
        assertThat(server.received()).isEmpty();
    }

    private static Endpoint deadEndpoint() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return Endpoint.of(Plane.MANAGEMENT, "http", "http://127.0.0.1:" + socket.getLocalPort());
        }
    }

    @Test
    void failsOverToTheNextEndpointAfterARequestThatWasNotSent() throws IOException {
        server.json("GET", "/api/overview", "overview.json");
        RabbitMqCluster both = FakeManagement.cluster(deadEndpoint(), server.endpoint());
        assertThat(both.management()
                        .get("/api/overview", Map.of(), deadline(), "read")
                        .body()
                        .isObject())
                .isTrue();
    }

    @Test
    void doesNotFailOverAfterARejection() {
        server.respond("GET", "/api/overview", 403, Map.of(), "{}");
        FakeManagement second = new FakeManagement();
        try (second) {
            second.json("GET", "/api/overview", "overview.json");
            RabbitMqCluster both = FakeManagement.cluster(server.endpoint(), second.endpoint());
            BrokerException e = failure(() -> both.management().get("/api/overview", Map.of(), deadline(), "x"));
            assertThat(e.status()).isEqualTo(Status.PERMISSION_DENIED);
            assertThat(second.received()).isEmpty();
        }
    }

    @Test
    void reportsUnreachableWhenEveryEndpointRefuses() throws IOException {
        RabbitMqCluster none = FakeManagement.cluster(deadEndpoint(), deadEndpoint());
        BrokerException e = failure(() -> none.management().get("/api/overview", Map.of(), deadline(), "x"));
        assertThat(e.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(e.reason()).isEqualTo(ErrorReasons.UNREACHABLE);
        assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
        assertThat(e.toString() + e.getMessage()).doesNotContain(SECRET);
    }

    @Test
    void takesOnePermitPerRequestAndCompletesItWithTheServerTime() {
        server.respond("GET", "/api/overview", 200, Map.of("Date", "Sun, 01 Nov 2026 10:00:00 GMT"), "{}");
        server.respond("GET", "/api/nodes", 500, Map.of(), "");
        List<CallInfo> calls = new ArrayList<>();
        List<Outcome> outcomes = new ArrayList<>();
        List<Optional<Instant>> times = new ArrayList<>();
        CallInterceptor interceptor = call -> {
            calls.add(call);
            return (outcome, serverTime) -> {
                outcomes.add(outcome);
                times.add(serverTime);
            };
        };
        RabbitMqCluster cluster = FakeManagement.cluster(
                Credentials.usernamePassword("ops", SECRET),
                ConnectOptions.defaults().withInterceptor(interceptor),
                server.endpoint());
        cluster.management().get("/api/overview", Map.of(), deadline(), "read overview");
        failure(() -> cluster.management().get("/api/nodes", Map.of(), deadline(), "read nodes"));
        assertThat(calls).hasSize(2);
        assertThat(calls.getFirst().operation()).isEqualTo("read overview");
        assertThat(calls.getFirst().plane()).isEqualTo(Plane.MANAGEMENT);
        assertThat(calls.getFirst().cost()).isEqualTo(1);
        assertThat(outcomes.get(0).success()).isTrue();
        assertThat(outcomes.get(1).success()).isFalse();
        assertThat(times.get(0)).isPresent();
        assertThat(times.get(1)).isEmpty();
    }

    @Test
    void aRefusedPermitSendsNothing() {
        BrokerException refusal = new BrokerException(
                Status.RESOURCE_EXHAUSTED, ErrorInfo.of(ErrorReasons.CALL_REFUSED), Dispatch.NOT_SENT, null, "no");
        RabbitMqCluster cluster = FakeManagement.cluster(
                Credentials.usernamePassword("ops", SECRET),
                ConnectOptions.defaults().withInterceptor(call -> {
                    throw refusal;
                }),
                server.endpoint(),
                server.endpoint());
        assertThat(failure(() -> cluster.management().get("/api/overview", Map.of(), deadline(), "x")))
                .isSameAs(refusal);
        assertThat(server.received()).isEmpty();
    }

    @Test
    void anHttpsEndpointNeedsTlsMaterial() {
        Endpoint https = Endpoint.of(Plane.MANAGEMENT, "https", "https://127.0.0.1:1");
        BrokerException e = failure(
                () -> FakeManagement.cluster(https).management().get("/api/overview", Map.of(), deadline(), "x"));
        assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT);
        assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
    }

    @Test
    void aClusterWithoutAManagementEndpointCannotCall() {
        BrokerException e =
                failure(() -> FakeManagement.cluster().management().get("/api/overview", Map.of(), deadline(), "x"));
        assertThat(e.reason()).isEqualTo(ErrorReasons.ENDPOINT_NOT_FOUND);
        assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
    }
}
