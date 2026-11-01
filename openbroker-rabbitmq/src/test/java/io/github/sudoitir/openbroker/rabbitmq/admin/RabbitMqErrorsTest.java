package io.github.sudoitir.openbroker.rabbitmq.admin;

import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.AuthenticationFailureException;
import com.rabbitmq.client.PossibleAuthenticationFailureException;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.AMQImpl;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpTimeoutException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLHandshakeException;
import org.junit.jupiter.api.Test;

class RabbitMqErrorsTest {

    private static final String SECRET = "s3cret-marker";

    private static void expect(BrokerException e, Status status, String reason, Dispatch dispatch) {
        assertThat(e.status()).isEqualTo(status);
        assertThat(e.reason()).isEqualTo(reason);
        assertThat(e.dispatch()).isEqualTo(dispatch);
    }

    private static BrokerException http(int status, String reason, boolean write) {
        return RabbitMqErrors.fromStatus(status, "op", "/api/queues/%2F/q", reason, write);
    }

    @Test
    void http401WithLoginFailedOrNoBodyIsRejectedCredentials() {
        BrokerException e = http(401, "Login failed", false);
        expect(e, Status.UNAUTHENTICATED, ErrorReasons.CREDENTIALS_REJECTED, Dispatch.REJECTED);
        assertThat(e.info().metadata()).containsEntry(ErrorReasons.PLANE, "MANAGEMENT");
        expect(http(401, "", false), Status.UNAUTHENTICATED, ErrorReasons.CREDENTIALS_REJECTED, Dispatch.REJECTED);
    }

    @Test
    void http401WithAnotherReasonAndHttp403AreNotAuthorized() {
        for (String reason : new String[] {"Not management user", "Not administrator user", "Access refused."}) {
            expect(http(401, reason, false), Status.PERMISSION_DENIED, ErrorReasons.NOT_AUTHORIZED, Dispatch.REJECTED);
        }
        expect(http(403, "", true), Status.PERMISSION_DENIED, ErrorReasons.NOT_AUTHORIZED, Dispatch.REJECTED);
    }

    @Test
    void http404OnTheOverviewIsAMissingEndpointAndOnAnObjectIsGone() {
        expect(
                RabbitMqErrors.fromStatus(404, "op", "/api/overview", "", false),
                Status.NOT_FOUND,
                ErrorReasons.ENDPOINT_NOT_FOUND,
                Dispatch.REJECTED);
        expect(
                RabbitMqErrors.fromStatus(404, "op", "/rmq/api/overview", "", false),
                Status.NOT_FOUND,
                ErrorReasons.ENDPOINT_NOT_FOUND,
                Dispatch.REJECTED);
        expect(http(404, "Not Found", false), Status.NOT_FOUND, ErrorReasons.RESOURCE_GONE, Dispatch.REJECTED);
    }

    @Test
    void http400ReadsItsReason() {
        expect(
                http(400, "PRECONDITION_FAILED - inequivalent arg 'durable'", true),
                Status.ALREADY_EXISTS,
                ErrorReasons.RESOURCE_EXISTS,
                Dispatch.REJECTED);
        expect(
                http(400, "Queue is not empty", true),
                Status.FAILED_PRECONDITION,
                ErrorReasons.NOT_EMPTY,
                Dispatch.REJECTED);
        expect(
                http(400, "Resource in use", true),
                Status.FAILED_PRECONDITION,
                ErrorReasons.HAS_CONSUMERS,
                Dispatch.REJECTED);
        expect(http(400, "bad", true), Status.INVALID_ARGUMENT, ErrorReasons.INVALID_ARGUMENT, Dispatch.REJECTED);
    }

    @Test
    void otherHttpStatuses() {
        expect(http(405, "", true), Status.UNIMPLEMENTED, ErrorReasons.NOT_SUPPORTED, Dispatch.REJECTED);
        expect(http(429, "", false), Status.RESOURCE_EXHAUSTED, ErrorReasons.RATE_LIMITED, Dispatch.REJECTED);
        expect(http(302, "", false), Status.FAILED_PRECONDITION, ErrorReasons.ENDPOINT_NOT_FOUND, Dispatch.REJECTED);
        expect(http(409, "", true), Status.FAILED_PRECONDITION, ErrorReasons.PRECONDITION_FAILED, Dispatch.REJECTED);
        expect(http(500, "", false), Status.INTERNAL, ErrorReasons.BAD_RESPONSE, Dispatch.REJECTED);
        expect(http(500, "", true), Status.INTERNAL, ErrorReasons.BAD_RESPONSE, Dispatch.IN_DOUBT);
        for (int code : new int[] {502, 503, 504}) {
            expect(http(code, "", true), Status.UNAVAILABLE, ErrorReasons.UNREACHABLE, Dispatch.IN_DOUBT);
            expect(http(code, "", false), Status.UNAVAILABLE, ErrorReasons.UNREACHABLE, Dispatch.REJECTED);
        }
        expect(http(507, "", true), Status.INTERNAL, ErrorReasons.BAD_RESPONSE, Dispatch.IN_DOUBT);
        assertThat(RabbitMqErrors.fromStatus(404, "op").info().metadata())
                .containsEntry(RabbitMqErrors.HTTP_STATUS, "404");
    }

    @Test
    void oversizedAndMalformedBodies() {
        expect(
                RabbitMqErrors.responseTooLarge(10),
                Status.RESOURCE_EXHAUSTED,
                ErrorReasons.RESPONSE_TOO_LARGE,
                Dispatch.IN_DOUBT);
        expect(RabbitMqErrors.badResponse("x", null), Status.INTERNAL, ErrorReasons.BAD_RESPONSE, Dispatch.IN_DOUBT);
        expect(
                RabbitMqErrors.unexpectedSystem("x"),
                Status.FAILED_PRECONDITION,
                ErrorReasons.UNEXPECTED_SYSTEM,
                Dispatch.REJECTED);
        expect(
                RabbitMqErrors.unsupportedVersion("x"),
                Status.FAILED_PRECONDITION,
                ErrorReasons.UNSUPPORTED_VERSION,
                Dispatch.REJECTED);
        expect(RabbitMqErrors.invalid("x"), Status.INVALID_ARGUMENT, ErrorReasons.INVALID_ARGUMENT, Dispatch.NOT_SENT);
        expect(
                RabbitMqErrors.noEndpoint("x"),
                Status.FAILED_PRECONDITION,
                ErrorReasons.ENDPOINT_NOT_FOUND,
                Dispatch.NOT_SENT);
        expect(RabbitMqErrors.unimplemented("x"), Status.UNIMPLEMENTED, ErrorReasons.NOT_SUPPORTED, Dispatch.NOT_SENT);
    }

    @Test
    void amqpAccessAndExistence() {
        expect(
                RabbitMqErrors.fromReply(530, "NOT_ALLOWED - access to vhost 'v' refused for user 'u'"),
                Status.PERMISSION_DENIED,
                ErrorReasons.NOT_AUTHORIZED,
                Dispatch.REJECTED);
        BrokerException refused = RabbitMqErrors.fromReply(
                403, "ACCESS_REFUSED - access to exchange 'ex' in vhost 'v' refused for user 'u'");
        expect(refused, Status.PERMISSION_DENIED, ErrorReasons.NOT_AUTHORIZED, Dispatch.REJECTED);
        assertThat(refused.info().metadata()).containsEntry(ErrorReasons.RESOURCE, "rabbitmq.exchange/v/ex");
        BrokerException gone = RabbitMqErrors.fromReply(
                404,
                "NOT_FOUND - no queue 'q' in vhost 'v'",
                Optional.of(ResourceId.of("rabbitmq.quorum-queue", "v", "q")));
        expect(gone, Status.NOT_FOUND, ErrorReasons.RESOURCE_GONE, Dispatch.REJECTED);
        assertThat(gone.info().metadata()).containsEntry(ErrorReasons.RESOURCE, "rabbitmq.quorum-queue/v/q");
        BrokerException defaultExchange = RabbitMqErrors.fromReply(404, "NOT_FOUND - no exchange '' in vhost '/'");
        assertThat(defaultExchange.info().metadata().get(ErrorReasons.RESOURCE)).contains("amq.default");
    }

    @Test
    void amqpPreconditions() {
        expect(
                RabbitMqErrors.fromReply(405, "RESOURCE_LOCKED - cannot obtain exclusive access"),
                Status.FAILED_PRECONDITION,
                ErrorReasons.HAS_CONSUMERS,
                Dispatch.REJECTED);
        expect(
                RabbitMqErrors.fromReply(406, "PRECONDITION_FAILED - inequivalent arg 'x-queue-type'"),
                Status.ALREADY_EXISTS,
                ErrorReasons.RESOURCE_EXISTS,
                Dispatch.REJECTED);
        expect(
                RabbitMqErrors.fromReply(406, "PRECONDITION_FAILED - delivery acknowledgement on channel 1 timed out"),
                Status.FAILED_PRECONDITION,
                ErrorReasons.DELIVERY_STALE,
                Dispatch.REJECTED);
        expect(
                RabbitMqErrors.fromReply(406, "PRECONDITION_FAILED - unknown delivery tag 7"),
                Status.FAILED_PRECONDITION,
                ErrorReasons.DELIVERY_STALE,
                Dispatch.REJECTED);
        expect(
                RabbitMqErrors.fromReply(406, "PRECONDITION_FAILED - other"),
                Status.FAILED_PRECONDITION,
                ErrorReasons.PRECONDITION_FAILED,
                Dispatch.REJECTED);
        expect(
                RabbitMqErrors.fromReply(312, "NO_ROUTE"),
                Status.FAILED_PRECONDITION,
                ErrorReasons.NOT_ROUTED,
                Dispatch.REJECTED);
        expect(
                RabbitMqErrors.nacked(Optional.empty()),
                Status.RESOURCE_EXHAUSTED,
                ErrorReasons.DESTINATION_FULL,
                Dispatch.REJECTED);
    }

    @Test
    void amqpServerFailures() {
        expect(
                RabbitMqErrors.fromReply(320, "CONNECTION_FORCED"),
                Status.UNAVAILABLE,
                ErrorReasons.UNREACHABLE,
                Dispatch.IN_DOUBT);
        BrokerException resource = RabbitMqErrors.fromReply(506, "RESOURCE_ERROR");
        assertThat(resource.status()).isEqualTo(Status.RESOURCE_EXHAUSTED);
        assertThat(resource.dispatch()).isEqualTo(Dispatch.REJECTED);
        expect(
                RabbitMqErrors.fromReply(540, "NOT_IMPLEMENTED"),
                Status.UNIMPLEMENTED,
                ErrorReasons.NOT_SUPPORTED,
                Dispatch.REJECTED);
        for (int code = 501; code <= 505; code++) {
            expect(RabbitMqErrors.fromReply(code, "x"), Status.INTERNAL, ErrorReasons.BAD_RESPONSE, Dispatch.IN_DOUBT);
        }
        BrokerException internal = RabbitMqErrors.fromReply(541, "INTERNAL_ERROR");
        assertThat(internal.status()).isEqualTo(Status.INTERNAL);
        assertThat(internal.dispatch()).isEqualTo(Dispatch.IN_DOUBT);
        assertThat(RabbitMqErrors.fromReply(999, null).dispatch()).isEqualTo(Dispatch.IN_DOUBT);
    }

    @Test
    void ioFailures() {
        expect(
                RabbitMqErrors.fromIo(new ConnectException("refused"), true),
                Status.UNAVAILABLE,
                ErrorReasons.UNREACHABLE,
                Dispatch.NOT_SENT);
        expect(
                RabbitMqErrors.fromIo(new IOException(new UnknownHostException("h")), true),
                Status.UNAVAILABLE,
                ErrorReasons.UNREACHABLE,
                Dispatch.NOT_SENT);
        expect(
                RabbitMqErrors.fromIo(new NoRouteToHostException("n"), true),
                Status.UNAVAILABLE,
                ErrorReasons.UNREACHABLE,
                Dispatch.NOT_SENT);
        expect(
                RabbitMqErrors.fromIo(new SSLHandshakeException("bad cert"), true),
                Status.UNAVAILABLE,
                ErrorReasons.TLS_HANDSHAKE_FAILED,
                Dispatch.NOT_SENT);
        expect(
                RabbitMqErrors.fromIo(new HttpConnectTimeoutException("c"), true),
                Status.DEADLINE_EXCEEDED,
                "DEADLINE_EXCEEDED",
                Dispatch.NOT_SENT);
        expect(
                RabbitMqErrors.fromIo(new HttpTimeoutException("t"), true),
                Status.DEADLINE_EXCEEDED,
                "DEADLINE_EXCEEDED",
                Dispatch.IN_DOUBT);
        expect(
                RabbitMqErrors.fromIo(new HttpTimeoutException("t"), false),
                Status.DEADLINE_EXCEEDED,
                "DEADLINE_EXCEEDED",
                Dispatch.NOT_SENT);
        expect(
                RabbitMqErrors.fromIo(new TimeoutException("t"), true),
                Status.DEADLINE_EXCEEDED,
                "DEADLINE_EXCEEDED",
                Dispatch.IN_DOUBT);
        expect(
                RabbitMqErrors.fromIo(new IOException("reset"), true),
                Status.UNAVAILABLE,
                ErrorReasons.UNREACHABLE,
                Dispatch.IN_DOUBT);
        expect(
                RabbitMqErrors.fromIo(new IOException("reset"), false),
                Status.UNAVAILABLE,
                ErrorReasons.UNREACHABLE,
                Dispatch.NOT_SENT);
        expect(
                RabbitMqErrors.fromIo(new IllegalStateException("x"), true),
                Status.INTERNAL,
                "INTERNAL",
                Dispatch.IN_DOUBT);
    }

    @Test
    void amqpClientExceptions() {
        expect(
                RabbitMqErrors.fromIo(new AuthenticationFailureException("ACCESS_REFUSED"), false),
                Status.UNAUTHENTICATED,
                ErrorReasons.CREDENTIALS_REJECTED,
                Dispatch.REJECTED);
        BrokerException possible =
                RabbitMqErrors.fromIo(new PossibleAuthenticationFailureException(new IOException("eof")), false);
        assertThat(possible.info().metadata()).containsEntry(ErrorReasons.PLANE, Plane.DATA.name());
        ShutdownSignalException channel = new ShutdownSignalException(
                false, false, new AMQImpl.Channel.Close(404, "NOT_FOUND - no queue 'q' in vhost '/'", 50, 10), null);
        expect(RabbitMqErrors.fromIo(channel, true), Status.NOT_FOUND, ErrorReasons.RESOURCE_GONE, Dispatch.REJECTED);
        ShutdownSignalException connection = new ShutdownSignalException(
                true, false, new AMQImpl.Connection.Close(320, "CONNECTION_FORCED", 0, 0), null);
        expect(
                RabbitMqErrors.fromIo(connection, true),
                Status.UNAVAILABLE,
                ErrorReasons.UNREACHABLE,
                Dispatch.IN_DOUBT);
        expect(
                RabbitMqErrors.fromIo(new ShutdownSignalException(true, false, null, null), true),
                Status.UNAVAILABLE,
                ErrorReasons.UNREACHABLE,
                Dispatch.IN_DOUBT);
    }

    @Test
    void noFailureHoldsASecretOrUserInformation() {
        BrokerException[] failures = {
            http(401, "Login failed", false),
            RabbitMqErrors.fromIo(new IOException("failed"), true),
            RabbitMqErrors.fromReply(403, "ACCESS_REFUSED - user 'u'"),
            RabbitMqErrors.credentialsRejected(Plane.MANAGEMENT, 401)
        };
        for (BrokerException e : failures) {
            assertThat(e.toString()).doesNotContain(SECRET);
            assertThat(e.info().toString()).doesNotContain(SECRET);
        }
        ClusterSpec userInfo = new ClusterSpec(
                List.of(),
                List.of(Endpoint.of(Plane.MANAGEMENT, "http", "http://ops:" + SECRET + "@host:15672")),
                Map.of());
        TestContextHolder.assertRefusedWithoutSecret(userInfo, SECRET);
    }

    private static final class TestContextHolder {
        static void assertRefusedWithoutSecret(ClusterSpec spec, String secret) {
            BrokerException e = null;
            try {
                FakeManagement.cluster(spec.clusterEndpoints().toArray(Endpoint[]::new));
            } catch (BrokerException refused) {
                e = refused;
            }
            assertThat(e).isNotNull();
            assertThat(e.getMessage()).doesNotContain(secret);
            assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT);
        }
    }
}
