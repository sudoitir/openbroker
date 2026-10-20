package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.artemis.JolokiaResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;

/**
 * The driver's failures, as the SDK's status and reasons. Messages are the operator-facing texts the
 * driver has always given; the native error goes in metadata under {@value #NATIVE_ERROR}.
 */
public final class Errors {

    /** Metadata key: the broker's own error text. */
    public static final String NATIVE_ERROR = "artemis.error";
    /** Metadata key: the broker's error code ({@code AMQ229017}) or Java class name. */
    public static final String NATIVE_CODE = "artemis.code";
    /** Metadata key: the address-setting a parse error names. */
    public static final String SETTING = "artemis.setting";

    public static final String UNREACHABLE_MESSAGE = "Nothing answered at this address.";
    public static final String CREDENTIALS_MESSAGE = "The broker rejected these credentials.";
    public static final String NOT_ARTEMIS_MESSAGE =
            "This is a Jolokia agent, but no Artemis broker is registered on it.";
    public static final String WRONG_PATH_MESSAGE =
            "No Jolokia agent here. The Artemis console usually serves it at /console/jolokia.";
    public static final String TLS_MESSAGE = "The TLS connection to the broker could not be established.";
    public static final String BAD_RESPONSE_MESSAGE = "The broker answered, but not with a Jolokia response.";
    public static final String UNSUPPORTED_VERSION_MESSAGE = "This Artemis release is older than this driver supports.";

    // Artemis error codes, measured on 2.44.0.
    static final String QUEUE_EXISTS = "AMQ229019";
    static final String QUEUE_ABSENT = "AMQ229017";
    static final String ADDRESS_ABSENT = "AMQ229203";
    static final String ADDRESS_EXISTS = "AMQ229204";
    static final String ADDRESS_HAS_BINDINGS = "AMQ229205";
    static final String ROUTING_TYPE_IMMUTABLE = "AMQ229211";
    static final String QUEUE_HAS_CONSUMERS = "AMQ229025";
    static final String INVALID_FILTER = "AMQ229020";
    static final String DIVERT_ABSENT = "AMQ229012";
    static final String SETTING_PARSE = "Error while parsing MetaData";
    static final String PAGE_SIZE_VS_MAX = "pageSize has to be lower than maxSizeBytes";
    /** The MBean is not registered: raised by JMX, so a Java class name rather than an AMQ code. */
    public static final String MBEAN_ABSENT = "InstanceNotFoundException";

    private static final Pattern SETTING_NAME = Pattern.compile("name='([^']*)'");

    private Errors() {}

    public static BrokerException of(
            Status status,
            String reason,
            Dispatch dispatch,
            @Nullable NodeId node,
            String message,
            Map<String, String> metadata) {
        return new BrokerException(status, ErrorInfo.of(reason, metadata), dispatch, node, message);
    }

    public static BrokerException of(
            Status status,
            String reason,
            Dispatch dispatch,
            @Nullable NodeId node,
            String message,
            @Nullable Throwable cause) {
        return new BrokerException(status, ErrorInfo.of(reason), dispatch, node, message, cause);
    }

    public static BrokerException unreachable(
            @Nullable NodeId node, String message, Dispatch dispatch, Throwable cause) {
        return of(Status.UNAVAILABLE, ErrorReasons.UNREACHABLE, dispatch, node, message, cause);
    }

    public static BrokerException credentialsRejected(
            @Nullable NodeId node, Plane plane, int httpStatus, String message) {
        return new BrokerException(
                httpStatus == 403 ? Status.PERMISSION_DENIED : Status.UNAUTHENTICATED,
                ErrorInfo.of(ErrorReasons.CREDENTIALS_REJECTED, Map.of(ErrorReasons.PLANE, plane.name())),
                Dispatch.REJECTED,
                node,
                message);
    }

    public static BrokerException notArtemis(@Nullable NodeId node) {
        return of(
                Status.FAILED_PRECONDITION,
                ErrorReasons.UNEXPECTED_SYSTEM,
                Dispatch.REJECTED,
                node,
                NOT_ARTEMIS_MESSAGE,
                (Throwable) null);
    }

    public static BrokerException wrongPath(@Nullable NodeId node, String message) {
        return of(
                Status.NOT_FOUND, ErrorReasons.ENDPOINT_NOT_FOUND, Dispatch.REJECTED, node, message, (Throwable) null);
    }

    public static BrokerException tls(@Nullable NodeId node, String message, @Nullable Throwable cause) {
        return of(Status.UNAVAILABLE, ErrorReasons.TLS_HANDSHAKE_FAILED, Dispatch.NOT_SENT, node, message, cause);
    }

    public static BrokerException badResponse(@Nullable NodeId node, String message, @Nullable Throwable cause) {
        return of(Status.INTERNAL, ErrorReasons.BAD_RESPONSE, Dispatch.IN_DOUBT, node, message, cause);
    }

    public static BrokerException responseTooLarge(@Nullable NodeId node, long limit) {
        return of(
                Status.RESOURCE_EXHAUSTED,
                ErrorReasons.RESPONSE_TOO_LARGE,
                Dispatch.IN_DOUBT,
                node,
                "The broker's answer was larger than the " + limit + "-byte limit.",
                (Throwable) null);
    }

    /**
     * A failed management response: a known refusal with its reason, or a bad response naming the
     * operation.
     */
    public static BrokerException refusal(@Nullable NodeId node, JolokiaResponse response, String operation) {
        String error = response.error();
        if (error != null) {
            BrokerException known = classify(node, error);
            if (known != null) {
                return known;
            }
        }
        return of(
                Status.INTERNAL,
                ErrorReasons.BAD_RESPONSE,
                Dispatch.REJECTED,
                node,
                operation + " failed: " + (error != null ? error : "status " + response.status()),
                error == null ? Map.of() : Map.of(NATIVE_ERROR, error));
    }

    /** A known management refusal, or null. */
    public static @Nullable BrokerException classify(@Nullable NodeId node, String error) {
        if (error.contains(QUEUE_ABSENT) || error.contains(ADDRESS_ABSENT) || error.contains(DIVERT_ABSENT)) {
            return refused(
                    Status.NOT_FOUND,
                    ErrorReasons.RESOURCE_GONE,
                    node,
                    "Already absent: " + error,
                    error,
                    code(error, QUEUE_ABSENT, ADDRESS_ABSENT, DIVERT_ABSENT));
        }
        if (error.contains(MBEAN_ABSENT)) {
            return refused(
                    Status.NOT_FOUND,
                    ErrorReasons.RESOURCE_GONE,
                    node,
                    "The queue or address is no longer on this node: " + error,
                    error,
                    MBEAN_ABSENT);
        }
        if (error.contains(QUEUE_EXISTS) || error.contains(ADDRESS_EXISTS)) {
            return refused(
                    Status.ALREADY_EXISTS,
                    ErrorReasons.RESOURCE_EXISTS,
                    node,
                    "Already present: " + error,
                    error,
                    code(error, QUEUE_EXISTS, ADDRESS_EXISTS));
        }
        if (error.contains(ADDRESS_HAS_BINDINGS)) {
            return refused(
                    Status.FAILED_PRECONDITION, ErrorReasons.HAS_CHILDREN, node, error, error, ADDRESS_HAS_BINDINGS);
        }
        if (error.contains(QUEUE_HAS_CONSUMERS)) {
            return refused(
                    Status.FAILED_PRECONDITION,
                    ErrorReasons.HAS_CONSUMERS,
                    node,
                    "The queue has consumers attached, and the broker will not delete it while they are.",
                    error,
                    QUEUE_HAS_CONSUMERS);
        }
        if (error.contains(ROUTING_TYPE_IMMUTABLE)) {
            return refused(
                    Status.INVALID_ARGUMENT,
                    ErrorReasons.IMMUTABLE_FIELD,
                    node,
                    "A queue's routing type cannot be changed on a live queue. Recreate the queue instead.",
                    error,
                    ROUTING_TYPE_IMMUTABLE);
        }
        if (error.contains(INVALID_FILTER)) {
            return refused(
                    Status.INVALID_ARGUMENT,
                    ErrorReasons.INVALID_FILTER,
                    node,
                    "Invalid message filter.",
                    error,
                    INVALID_FILTER);
        }
        if (error.contains(SETTING_PARSE)) {
            Matcher name = SETTING_NAME.matcher(error);
            String field = null;
            while (name.find()) {
                field = name.group(1);
            }
            Map<String, String> metadata = new HashMap<>(Map.of(NATIVE_ERROR, error, NATIVE_CODE, SETTING_PARSE));
            if (field != null) {
                metadata.put(SETTING, field);
            }
            return of(
                    Status.INVALID_ARGUMENT,
                    ErrorReasons.INVALID_ARGUMENT,
                    Dispatch.REJECTED,
                    node,
                    "The broker could not parse the value of " + (field == null ? "an address-setting key" : field)
                            + ".",
                    metadata);
        }
        if (error.contains(PAGE_SIZE_VS_MAX)) {
            return refused(
                    Status.INVALID_ARGUMENT,
                    ErrorReasons.INVALID_ARGUMENT,
                    node,
                    "page-size-bytes must be lower than max-size-bytes; the broker refused the pair.",
                    error,
                    PAGE_SIZE_VS_MAX);
        }
        return null;
    }

    private static String code(String error, String... codes) {
        for (String c : codes) {
            if (error.contains(c)) {
                return c;
            }
        }
        return "";
    }

    private static BrokerException refused(
            Status status, String reason, @Nullable NodeId node, String message, String error, String code) {
        return of(status, reason, Dispatch.REJECTED, node, message, Map.of(NATIVE_ERROR, error, NATIVE_CODE, code));
    }
}
