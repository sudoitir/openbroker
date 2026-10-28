package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Status;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.jspecify.annotations.Nullable;

/**
 * The driver's failures, as the SDK's status and reasons. The native error goes in metadata under
 * {@value #NATIVE_ERROR} and its class under {@value #NATIVE_CODE}; a client configuration error
 * carries neither, because its text can quote a setting that holds a secret.
 */
public final class KafkaErrors {

    /** Metadata key: the cluster's own error text. */
    public static final String NATIVE_ERROR = "kafka.error";
    /** Metadata key: the Kafka exception's class name. */
    public static final String NATIVE_CODE = "kafka.code";

    private KafkaErrors() {}

    /** A refusal of a request that was never sent. */
    public static BrokerException refused(Status status, String reason, String message) {
        return new BrokerException(status, ErrorInfo.of(reason), Dispatch.NOT_SENT, null, message);
    }

    /** The failure for what Kafka cannot do: unimplemented, {@code NOT_SUPPORTED}, and not sent. */
    public static BrokerException unimplemented(String what) {
        return refused(Status.UNIMPLEMENTED, ErrorReasons.NOT_SUPPORTED, "Kafka cannot " + what + ".");
    }

    /** The SDK's failure for what a Kafka client threw. */
    public static BrokerException of(Throwable failure, @Nullable NodeId node) {
        if (failure instanceof BrokerException e) {
            return e;
        }
        if (failure instanceof ConfigException) {
            return new BrokerException(
                    Status.INVALID_ARGUMENT,
                    ErrorInfo.of(ErrorReasons.INVALID_ARGUMENT),
                    Dispatch.NOT_SENT,
                    node,
                    "The Kafka client rejected the connection settings.");
        }
        Mapped mapped = map(failure);
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put(NATIVE_CODE, failure.getClass().getSimpleName());
        if (failure.getMessage() != null) {
            metadata.put(NATIVE_ERROR, failure.getMessage());
        }
        return new BrokerException(
                mapped.status,
                ErrorInfo.of(mapped.reason, metadata),
                mapped.dispatch,
                node,
                mapped.message + (failure.getMessage() == null ? "" : ": " + failure.getMessage()),
                failure);
    }

    private static Mapped map(Throwable failure) {
        return switch (failure) {
            case SslAuthenticationException e ->
                new Mapped(
                        Status.UNAVAILABLE,
                        ErrorReasons.TLS_HANDSHAKE_FAILED,
                        Dispatch.NOT_SENT,
                        "The TLS connection to the cluster could not be established");
            case AuthenticationException e ->
                new Mapped(
                        Status.UNAUTHENTICATED,
                        ErrorReasons.CREDENTIALS_REJECTED,
                        Dispatch.NOT_SENT,
                        "The cluster rejected these credentials");
            case AuthorizationException e ->
                new Mapped(
                        Status.PERMISSION_DENIED,
                        ErrorReasons.NOT_AUTHORIZED,
                        Dispatch.REJECTED,
                        "The account is not authorized for this");
            case UnsupportedVersionException e ->
                new Mapped(
                        Status.UNIMPLEMENTED,
                        ErrorReasons.UNSUPPORTED_VERSION,
                        Dispatch.NOT_SENT,
                        "The cluster does not support this request");
            case UnknownTopicOrPartitionException e ->
                new Mapped(
                        Status.NOT_FOUND, ErrorReasons.RESOURCE_GONE, Dispatch.REJECTED, "The resource does not exist");
            case GroupIdNotFoundException e ->
                new Mapped(Status.NOT_FOUND, ErrorReasons.RESOURCE_GONE, Dispatch.REJECTED, "The group does not exist");
            case TopicExistsException e ->
                new Mapped(
                        Status.ALREADY_EXISTS,
                        ErrorReasons.RESOURCE_EXISTS,
                        Dispatch.REJECTED,
                        "The topic already exists");
            case InvalidConfigurationException e ->
                new Mapped(
                        Status.INVALID_ARGUMENT,
                        ErrorReasons.INVALID_ARGUMENT,
                        Dispatch.REJECTED,
                        "The cluster refused a setting");
            case InvalidRequestException e ->
                new Mapped(
                        Status.INVALID_ARGUMENT,
                        ErrorReasons.INVALID_ARGUMENT,
                        Dispatch.REJECTED,
                        "The cluster refused the request");
            case InvalidPartitionsException e ->
                new Mapped(
                        Status.INVALID_ARGUMENT,
                        ErrorReasons.INVALID_ARGUMENT,
                        Dispatch.REJECTED,
                        "The partition count is invalid");
            case TimeoutException _ -> unreachable();
            case DisconnectException _ -> unreachable();
            case NetworkException _ -> unreachable();
            // A leader that moved or is not yet elected, too few replicas in sync: the cluster could not take the
            // write.
            case RetriableException _ -> unreachable();
            default ->
                new Mapped(
                        Status.INTERNAL,
                        ErrorReasons.UNEXPECTED_SYSTEM,
                        Dispatch.REJECTED,
                        "The cluster answered unexpectedly");
        };
    }

    private static Mapped unreachable() {
        return new Mapped(Status.UNAVAILABLE, ErrorReasons.UNREACHABLE, Dispatch.IN_DOUBT, "Nothing answered in time");
    }

    private record Mapped(Status status, String reason, Dispatch dispatch, String message) {}
}
