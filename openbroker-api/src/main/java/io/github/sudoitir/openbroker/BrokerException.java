package io.github.sudoitir.openbroker;

import java.util.Objects;
import java.util.Optional;
import org.jspecify.annotations.Nullable;

/**
 * Every failure of an OpenBroker call: a canonical {@link Status}, the precise {@link ErrorInfo}, the
 * {@link Dispatch} that says whether a retry is safe, and the node when one is known. The message
 * never contains a secret.
 */
public class BrokerException extends RuntimeException {

    private final Status status;
    private final ErrorInfo info;
    private final Dispatch dispatch;
    private final @Nullable NodeId node;

    public BrokerException(Status status, ErrorInfo info, Dispatch dispatch, @Nullable NodeId node, String message) {
        this(status, info, dispatch, node, message, null);
    }

    public BrokerException(
            Status status,
            ErrorInfo info,
            Dispatch dispatch,
            @Nullable NodeId node,
            String message,
            @Nullable Throwable cause) {
        super(message, cause);
        this.status = Objects.requireNonNull(status, "status");
        this.info = Objects.requireNonNull(info, "info");
        this.dispatch = Objects.requireNonNull(dispatch, "dispatch");
        this.node = node;
    }

    public Status status() {
        return status;
    }

    public ErrorInfo info() {
        return info;
    }

    public String reason() {
        return info.reason();
    }

    public Dispatch dispatch() {
        return dispatch;
    }

    public Optional<NodeId> node() {
        return Optional.ofNullable(node);
    }

    /** Whether repeating the same request cannot apply it twice. */
    public boolean safeToRetry() {
        return dispatch != Dispatch.IN_DOUBT;
    }

    /** The same failure, attributed to a node. */
    public BrokerException at(NodeId node) {
        BrokerException copy = new BrokerException(status, info, dispatch, node, getMessage(), getCause());
        copy.setStackTrace(getStackTrace());
        return copy;
    }

    @Override
    public String toString() {
        return "BrokerException[" + status + "/" + info.reason() + (node == null ? "" : " @" + node) + "]: "
                + getMessage();
    }
}
