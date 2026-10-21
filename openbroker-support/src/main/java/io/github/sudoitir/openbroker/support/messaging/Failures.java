package io.github.sudoitir.openbroker.support.messaging;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Status;
import org.jspecify.annotations.Nullable;

/** The failures the engine reports, with the status, reason and dispatch the contract fixes. */
final class Failures {

    private Failures() {}

    static BrokerException bufferFull(NodeId node) {
        return new BrokerException(
                Status.RESOURCE_EXHAUSTED,
                ErrorInfo.of(ErrorReasons.SEND_BUFFER_FULL),
                Dispatch.NOT_SENT,
                node,
                "The send buffer stayed full for the whole wait");
    }

    static BrokerException unavailable(NodeId node, Dispatch dispatch, String message, @Nullable Throwable cause) {
        return new BrokerException(
                Status.UNAVAILABLE, ErrorInfo.of(ErrorReasons.UNREACHABLE), dispatch, node, message, cause);
    }

    static BrokerException rolledBack(NodeId node) {
        return new BrokerException(
                Status.ABORTED,
                ErrorInfo.of(ErrorReasons.TRANSACTION_ROLLED_BACK),
                Dispatch.REJECTED,
                node,
                "The link was lost and the transaction was rolled back");
    }

    static BrokerException closed(NodeId node) {
        return new BrokerException(
                Status.FAILED_PRECONDITION,
                ErrorInfo.of(ErrorReasons.PRECONDITION_FAILED),
                Dispatch.NOT_SENT,
                node,
                "The sender is closed");
    }

    static BrokerException cancelled(NodeId node, Dispatch dispatch, @Nullable Throwable cause) {
        return new BrokerException(
                Status.CANCELLED, ErrorInfo.of("CANCELLED"), dispatch, node, "The call was cancelled", cause);
    }

    static BrokerException unorderedResend(NodeId node) {
        return new BrokerException(
                Status.INVALID_ARGUMENT,
                ErrorInfo.of(ErrorReasons.INVALID_ARGUMENT),
                Dispatch.NOT_SENT,
                node,
                "Ordering needs an idempotent sender with AT_LEAST_ONCE: a resend would reorder messages");
    }

    static BrokerException unexpected(NodeId node, RuntimeException cause) {
        return new BrokerException(
                Status.UNKNOWN,
                ErrorInfo.of("UNKNOWN"),
                Dispatch.IN_DOUBT,
                node,
                "The writer failed unexpectedly",
                cause);
    }
}
