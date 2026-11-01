package io.github.sudoitir.openbroker.rabbitmq.admin;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AuthenticationFailureException;
import com.rabbitmq.client.PossibleAuthenticationFailureException;
import com.rabbitmq.client.ShutdownSignalException;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpTimeoutException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.SSLException;
import org.jspecify.annotations.Nullable;

/**
 * The whole mapping of RabbitMQ failures (HTTP status, AMQP reply code, IO, TLS) to {@link BrokerException}, and the
 * only place one is built. No message, {@code toString()} or metadata holds a password, a token or the user
 * information of a URI: messages name an operation, a status or a code and the server's own short reason, never a
 * request.
 */
public final class RabbitMqErrors {

    /** Metadata key: the HTTP status the management API answered. */
    public static final String HTTP_STATUS = "rabbitmq.http_status";

    /** Metadata key: the AMQP reply code of a channel or connection close. */
    public static final String REPLY_CODE = "rabbitmq.reply_code";

    // AMQP 0-9-1 reply codes (RabbitMQ's constants).
    private static final int NO_ROUTE = 312;
    private static final int CONNECTION_FORCED = 320;
    private static final int ACCESS_REFUSED = 403;
    private static final int NOT_FOUND = 404;
    private static final int RESOURCE_LOCKED = 405;
    private static final int PRECONDITION_FAILED = 406;
    private static final int FRAME_ERROR_FIRST = 501;
    private static final int FRAME_ERROR_LAST = 505;
    private static final int RESOURCE_ERROR = 506;
    private static final int NOT_ALLOWED = 530;
    private static final int NOT_IMPLEMENTED = 540;
    private static final int INTERNAL_ERROR = 541;

    private static final int MAX_REASON = 200;
    private static final String OVERVIEW = "/api/overview";
    private static final Pattern VHOST = Pattern.compile("vhost '([^']*)'");
    private static final Pattern EXCHANGE = Pattern.compile("exchange '([^']*)'");

    private RabbitMqErrors() {}

    /** The failure of a part of the driver that a later change fills in. */
    public static BrokerException unimplemented(String what) {
        return build(
                Status.UNIMPLEMENTED,
                ErrorReasons.NOT_SUPPORTED,
                Dispatch.NOT_SENT,
                Map.of(),
                "Not implemented yet: " + what + ".",
                null);
    }

    /** The failure for a management API response with {@code httpStatus}, to the read {@code operation}. */
    public static BrokerException fromStatus(int httpStatus, String operation) {
        return fromStatus(httpStatus, operation, "", "", false);
    }

    /**
     * The failure for a management API response.
     *
     * @param httpStatus the status line's code
     * @param operation what the caller was doing, for the message
     * @param path the request path, which tells {@code /api/overview} from an object
     * @param serverReason the {@code reason} of the JSON error body, or empty
     * @param write whether the request changes state, which decides IN_DOUBT for 5xx
     */
    public static BrokerException fromStatus(
            int httpStatus, String operation, String path, String serverReason, boolean write) {
        String reason = serverReason == null ? "" : cap(serverReason);
        String lower = reason.toLowerCase(Locale.ROOT);
        String said = reason.isEmpty() ? "" : " (" + reason + ")";
        String text = "RabbitMQ answered " + httpStatus + " to " + operation + said + ".";
        Map<String, String> meta = Map.of(HTTP_STATUS, Integer.toString(httpStatus));
        Dispatch unsure = write ? Dispatch.IN_DOUBT : Dispatch.REJECTED;
        if (httpStatus >= 300 && httpStatus < 400) {
            return build(
                    Status.FAILED_PRECONDITION,
                    ErrorReasons.ENDPOINT_NOT_FOUND,
                    Dispatch.REJECTED,
                    meta,
                    "The management endpoint redirects (" + httpStatus + "); give the final URL.",
                    null);
        }
        return switch (httpStatus) {
            case 401 ->
                lower.isEmpty() || lower.contains("login failed")
                        ? credentialsRejected(Plane.MANAGEMENT, httpStatus)
                        : build(
                                Status.PERMISSION_DENIED,
                                ErrorReasons.NOT_AUTHORIZED,
                                Dispatch.REJECTED,
                                meta,
                                text,
                                null);
            case 403 ->
                build(Status.PERMISSION_DENIED, ErrorReasons.NOT_AUTHORIZED, Dispatch.REJECTED, meta, text, null);
            case 404 ->
                path != null && path.endsWith(OVERVIEW)
                        ? build(
                                Status.NOT_FOUND,
                                ErrorReasons.ENDPOINT_NOT_FOUND,
                                Dispatch.REJECTED,
                                meta,
                                "No RabbitMQ management API answers at this address (404 for " + OVERVIEW
                                        + "). Is the rabbitmq_management plugin enabled, and the path prefix right?",
                                null)
                        : build(Status.NOT_FOUND, ErrorReasons.RESOURCE_GONE, Dispatch.REJECTED, meta, text, null);
            case 400 -> badRequest(lower, meta, text);
            case 405 -> build(Status.UNIMPLEMENTED, ErrorReasons.NOT_SUPPORTED, Dispatch.REJECTED, meta, text, null);
            case 429 ->
                build(Status.RESOURCE_EXHAUSTED, ErrorReasons.RATE_LIMITED, Dispatch.REJECTED, meta, text, null);
            case 500 -> build(Status.INTERNAL, ErrorReasons.BAD_RESPONSE, unsure, meta, text, null);
            case 502, 503, 504 -> build(Status.UNAVAILABLE, ErrorReasons.UNREACHABLE, unsure, meta, text, null);
            default ->
                httpStatus >= 500
                        ? build(Status.INTERNAL, ErrorReasons.BAD_RESPONSE, unsure, meta, text, null)
                        : build(
                                Status.FAILED_PRECONDITION,
                                ErrorReasons.PRECONDITION_FAILED,
                                Dispatch.REJECTED,
                                meta,
                                text,
                                null);
        };
    }

    private static BrokerException badRequest(String lower, Map<String, String> meta, String text) {
        if (lower.contains("inequivalent arg")) {
            return build(Status.ALREADY_EXISTS, ErrorReasons.RESOURCE_EXISTS, Dispatch.REJECTED, meta, text, null);
        }
        if (lower.contains("not empty")) {
            return build(Status.FAILED_PRECONDITION, ErrorReasons.NOT_EMPTY, Dispatch.REJECTED, meta, text, null);
        }
        if (lower.contains("in use")) {
            return build(Status.FAILED_PRECONDITION, ErrorReasons.HAS_CONSUMERS, Dispatch.REJECTED, meta, text, null);
        }
        return build(Status.INVALID_ARGUMENT, ErrorReasons.INVALID_ARGUMENT, Dispatch.REJECTED, meta, text, null);
    }

    /** The failure for an AMQP channel or connection close with {@code replyCode}. */
    public static BrokerException fromReply(int replyCode, String replyText) {
        return fromReply(replyCode, replyText, Optional.empty());
    }

    /**
     * The failure for an AMQP close. {@code resource} is the resource the caller was working on, named in the
     * failure when the code concerns it; without it an exchange named by the reply text is used.
     */
    public static BrokerException fromReply(int replyCode, String replyText, Optional<ResourceId> resource) {
        String native_ = replyText == null ? "" : cap(replyText);
        String lower = native_.toLowerCase(Locale.ROOT);
        String text = "RabbitMQ closed the AMQP " + (native_.isEmpty() ? "channel" : "channel (" + native_ + ")")
                + " with reply code " + replyCode + ".";
        Map<String, String> meta = new HashMap<>();
        meta.put(REPLY_CODE, Integer.toString(replyCode));
        Optional<ResourceId> named = resource.or(() -> exchangeIn(native_));
        switch (replyCode) {
            case NOT_ALLOWED -> {
                vhostIn(native_).ifPresent(v -> meta.put("openbroker.vhost", v));
                return build(
                        Status.PERMISSION_DENIED, ErrorReasons.NOT_AUTHORIZED, Dispatch.REJECTED, meta, text, null);
            }
            case ACCESS_REFUSED -> {
                named.ifPresent(r -> meta.put(ErrorReasons.RESOURCE, r.format()));
                return build(
                        Status.PERMISSION_DENIED, ErrorReasons.NOT_AUTHORIZED, Dispatch.REJECTED, meta, text, null);
            }
            case NOT_FOUND -> {
                named.ifPresent(r -> meta.put(ErrorReasons.RESOURCE, r.format()));
                return build(Status.NOT_FOUND, ErrorReasons.RESOURCE_GONE, Dispatch.REJECTED, meta, text, null);
            }
            case RESOURCE_LOCKED -> {
                return build(
                        Status.FAILED_PRECONDITION, ErrorReasons.HAS_CONSUMERS, Dispatch.REJECTED, meta, text, null);
            }
            case PRECONDITION_FAILED -> {
                return preconditionFailed(lower, meta, text);
            }
            case NO_ROUTE -> {
                named.ifPresent(r -> meta.put(ErrorReasons.RESOURCE, r.format()));
                return build(Status.FAILED_PRECONDITION, ErrorReasons.NOT_ROUTED, Dispatch.REJECTED, meta, text, null);
            }
            case CONNECTION_FORCED -> {
                return build(Status.UNAVAILABLE, ErrorReasons.UNREACHABLE, Dispatch.IN_DOUBT, meta, text, null);
            }
            case RESOURCE_ERROR -> {
                return build(
                        Status.RESOURCE_EXHAUSTED,
                        Status.RESOURCE_EXHAUSTED.name(),
                        Dispatch.REJECTED,
                        meta,
                        text,
                        null);
            }
            case NOT_IMPLEMENTED -> {
                return build(Status.UNIMPLEMENTED, ErrorReasons.NOT_SUPPORTED, Dispatch.REJECTED, meta, text, null);
            }
            case INTERNAL_ERROR -> {
                return build(Status.INTERNAL, Status.INTERNAL.name(), Dispatch.IN_DOUBT, meta, text, null);
            }
            default -> {
                if (replyCode >= FRAME_ERROR_FIRST && replyCode <= FRAME_ERROR_LAST) {
                    return build(Status.INTERNAL, ErrorReasons.BAD_RESPONSE, Dispatch.IN_DOUBT, meta, text, null);
                }
                return build(Status.INTERNAL, Status.INTERNAL.name(), Dispatch.IN_DOUBT, meta, text, null);
            }
        }
    }

    private static BrokerException preconditionFailed(String lower, Map<String, String> meta, String text) {
        if (lower.contains("inequivalent arg")) {
            return build(Status.ALREADY_EXISTS, ErrorReasons.RESOURCE_EXISTS, Dispatch.REJECTED, meta, text, null);
        }
        if ((lower.contains("delivery acknowledgement") && lower.contains("timed out"))
                || lower.contains("unknown delivery tag")) {
            return build(Status.FAILED_PRECONDITION, ErrorReasons.DELIVERY_STALE, Dispatch.REJECTED, meta, text, null);
        }
        return build(Status.FAILED_PRECONDITION, ErrorReasons.PRECONDITION_FAILED, Dispatch.REJECTED, meta, text, null);
    }

    /** The failure for a publisher confirm that came back negative ({@code basic.nack}). */
    public static BrokerException nacked(Optional<ResourceId> destination) {
        Map<String, String> meta = new HashMap<>();
        destination.ifPresent(r -> meta.put(ErrorReasons.RESOURCE, r.format()));
        return build(
                Status.RESOURCE_EXHAUSTED,
                ErrorReasons.DESTINATION_FULL,
                Dispatch.REJECTED,
                meta,
                "RabbitMQ refused the message (basic.nack): the destination cannot take it.",
                null);
    }

    /** The failure for a credential rejection on {@code plane}. */
    public static BrokerException credentialsRejected(Plane plane, int httpStatus) {
        Map<String, String> meta = new HashMap<>();
        meta.put(ErrorReasons.PLANE, plane.name());
        if (httpStatus > 0) {
            meta.put(HTTP_STATUS, Integer.toString(httpStatus));
        }
        return build(
                Status.UNAUTHENTICATED,
                ErrorReasons.CREDENTIALS_REJECTED,
                Dispatch.REJECTED,
                meta,
                "RabbitMQ rejected these credentials for the " + plane.name().toLowerCase(Locale.ROOT) + " plane.",
                null);
    }

    /** The failure for an I/O or TLS error; {@code sent} says whether the request may have reached the system. */
    public static BrokerException fromIo(Throwable cause, boolean sent) {
        Dispatch afterWrite = sent ? Dispatch.IN_DOUBT : Dispatch.NOT_SENT;
        if (find(cause, PossibleAuthenticationFailureException.class) != null
                || find(cause, AuthenticationFailureException.class) != null) {
            BrokerException failure = credentialsRejected(Plane.DATA, 0);
            return build(
                    failure.status(),
                    failure.reason(),
                    failure.dispatch(),
                    failure.info().metadata(),
                    failure.getMessage(),
                    cause);
        }
        ShutdownSignalException shutdown = find(cause, ShutdownSignalException.class);
        if (shutdown != null) {
            return fromShutdown(shutdown);
        }
        if (find(cause, SSLException.class) != null) {
            return build(
                    Status.UNAVAILABLE,
                    ErrorReasons.TLS_HANDSHAKE_FAILED,
                    Dispatch.NOT_SENT,
                    Map.of(),
                    "The TLS connection to RabbitMQ could not be established: "
                            + rootMessage(find(cause, SSLException.class)),
                    cause);
        }
        if (find(cause, HttpConnectTimeoutException.class) != null) {
            return build(
                    Status.DEADLINE_EXCEEDED,
                    Status.DEADLINE_EXCEEDED.name(),
                    Dispatch.NOT_SENT,
                    Map.of(),
                    "RabbitMQ did not accept the connection in time.",
                    cause);
        }
        if (find(cause, HttpTimeoutException.class) != null || find(cause, TimeoutException.class) != null) {
            return build(
                    Status.DEADLINE_EXCEEDED,
                    Status.DEADLINE_EXCEEDED.name(),
                    afterWrite,
                    Map.of(),
                    "RabbitMQ did not answer in time.",
                    cause);
        }
        if (find(cause, ConnectException.class) != null) {
            return unreachable("Connection refused: nothing is listening at the address.", Dispatch.NOT_SENT, cause);
        }
        if (find(cause, UnknownHostException.class) != null) {
            return unreachable("The host name of the address does not resolve.", Dispatch.NOT_SENT, cause);
        }
        if (find(cause, NoRouteToHostException.class) != null) {
            return unreachable("There is no route to the address.", Dispatch.NOT_SENT, cause);
        }
        if (find(cause, IOException.class) != null) {
            return unreachable("The connection to RabbitMQ failed: " + rootMessage(cause), afterWrite, cause);
        }
        return build(
                Status.INTERNAL,
                Status.INTERNAL.name(),
                Dispatch.IN_DOUBT,
                Map.of(),
                "Unexpected failure: " + rootMessage(cause),
                cause);
    }

    private static BrokerException fromShutdown(ShutdownSignalException shutdown) {
        Object reason = shutdown.getReason();
        if (reason instanceof AMQP.Connection.Close close) {
            return withCause(fromReply(close.getReplyCode(), close.getReplyText()), shutdown);
        }
        if (reason instanceof AMQP.Channel.Close close) {
            return withCause(fromReply(close.getReplyCode(), close.getReplyText()), shutdown);
        }
        return unreachable("The AMQP connection to RabbitMQ was lost.", Dispatch.IN_DOUBT, shutdown);
    }

    private static BrokerException withCause(BrokerException failure, Throwable cause) {
        return build(
                failure.status(),
                failure.reason(),
                failure.dispatch(),
                failure.info().metadata(),
                failure.getMessage(),
                cause);
    }

    /** The failure for a body larger than the cap. */
    static BrokerException responseTooLarge(long maxBytes) {
        return build(
                Status.RESOURCE_EXHAUSTED,
                ErrorReasons.RESPONSE_TOO_LARGE,
                Dispatch.IN_DOUBT,
                Map.of(),
                "The response is larger than " + maxBytes + " bytes.",
                null);
    }

    /** The failure for a 200 that is not JSON, or JSON of the wrong shape. */
    static BrokerException badResponse(String message, @Nullable Throwable cause) {
        return build(
                Status.INTERNAL,
                ErrorReasons.BAD_RESPONSE,
                Dispatch.IN_DOUBT,
                Map.of(HTTP_STATUS, "200"),
                message,
                cause);
    }

    /** The failure for an answer that is not RabbitMQ's. */
    static BrokerException unexpectedSystem(String message) {
        return build(
                Status.FAILED_PRECONDITION, ErrorReasons.UNEXPECTED_SYSTEM, Dispatch.REJECTED, Map.of(), message, null);
    }

    /** The failure for a cluster outside the supported versions. */
    static BrokerException unsupportedVersion(String message) {
        return build(
                Status.FAILED_PRECONDITION,
                ErrorReasons.UNSUPPORTED_VERSION,
                Dispatch.REJECTED,
                Map.of(),
                message,
                null);
    }

    /** The failure for a request that cannot be sent as given. */
    public static BrokerException invalid(String message) {
        return build(
                Status.INVALID_ARGUMENT, ErrorReasons.INVALID_ARGUMENT, Dispatch.NOT_SENT, Map.of(), message, null);
    }

    /** The failure for a call that has no management endpoint to go to. */
    static BrokerException noEndpoint(String message) {
        return build(
                Status.FAILED_PRECONDITION,
                ErrorReasons.ENDPOINT_NOT_FOUND,
                Dispatch.NOT_SENT,
                Map.of(),
                message,
                null);
    }

    private static BrokerException unreachable(String message, Dispatch dispatch, Throwable cause) {
        return build(Status.UNAVAILABLE, ErrorReasons.UNREACHABLE, dispatch, Map.of(), message, cause);
    }

    /** Names {@code node} in {@code failure}. */
    static BrokerException at(BrokerException failure, @Nullable NodeId node) {
        return node == null ? failure : failure.at(node);
    }

    private static BrokerException build(
            Status status,
            String reason,
            Dispatch dispatch,
            Map<String, String> metadata,
            String message,
            @Nullable Throwable cause) {
        return new BrokerException(status, ErrorInfo.of(reason, metadata), dispatch, null, message, cause);
    }

    private static Optional<ResourceId> exchangeIn(String text) {
        Matcher vhost = VHOST.matcher(text);
        Matcher exchange = EXCHANGE.matcher(text);
        if (vhost.find() && exchange.find()) {
            String name = exchange.group(1).isEmpty() ? ManagementPaths.DEFAULT_EXCHANGE : exchange.group(1);
            return Optional.of(ResourceId.of(RabbitMqKinds.EXCHANGE.value(), vhost.group(1), name));
        }
        return Optional.empty();
    }

    private static Optional<String> vhostIn(String text) {
        Matcher vhost = VHOST.matcher(text);
        return vhost.find() ? Optional.of(vhost.group(1)) : Optional.empty();
    }

    private static <T extends Throwable> @Nullable T find(@Nullable Throwable cause, Class<T> type) {
        for (Throwable c = cause; c != null; c = c.getCause() == c ? null : c.getCause()) {
            if (type.isInstance(c)) {
                return type.cast(c);
            }
        }
        return null;
    }

    private static String rootMessage(@Nullable Throwable e) {
        Throwable root = e;
        while (root != null && root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        if (root == null) {
            return "";
        }
        return root.getMessage() != null
                ? cap(root.getMessage())
                : root.getClass().getSimpleName();
    }

    private static String cap(String text) {
        String flat = text.replaceAll("\\s+", " ").trim();
        return flat.length() > MAX_REASON ? flat.substring(0, MAX_REASON) + "..." : flat;
    }
}
