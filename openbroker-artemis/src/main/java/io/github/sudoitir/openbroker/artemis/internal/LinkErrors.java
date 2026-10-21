package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Status;
import org.apache.activemq.artemis.api.core.ActiveMQConnectionTimedOutException;
import org.apache.activemq.artemis.api.core.ActiveMQDisconnectedException;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQNotConnectedException;
import org.apache.activemq.artemis.api.core.ActiveMQObjectClosedException;
import org.apache.activemq.artemis.api.core.ActiveMQUnBlockedException;
import org.jspecify.annotations.Nullable;

/** The failures of a link that is lost, recovering or closed, with the status and dispatch the contract fixes. */
final class LinkErrors {

    private LinkErrors() {}

    /** The link is down and nothing was sent: a sender keeps the message for the recovery. */
    static BrokerException down(NodeId node, String message, @Nullable Throwable cause) {
        return new BrokerException(
                Status.UNAVAILABLE, ErrorInfo.of(ErrorReasons.UNREACHABLE), Dispatch.NOT_SENT, node, message, cause);
    }

    static BrokerException closed(NodeId node, String what) {
        return new BrokerException(
                Status.FAILED_PRECONDITION,
                ErrorInfo.of(ErrorReasons.PRECONDITION_FAILED),
                Dispatch.NOT_SENT,
                node,
                "The " + what + " is closed.");
    }

    /** A message handed out before the link recovered; the broker delivers it again. */
    static BrokerException stale(NodeId node) {
        return new BrokerException(
                Status.FAILED_PRECONDITION,
                ErrorInfo.of(ErrorReasons.DELIVERY_STALE),
                Dispatch.NOT_SENT,
                node,
                "The link recovered after this message was delivered; the broker delivers it again.");
    }

    /** A transaction the loss of its link ended. */
    static BrokerException rolledBack(NodeId node) {
        return new BrokerException(
                Status.ABORTED,
                ErrorInfo.of(ErrorReasons.TRANSACTION_ROLLED_BACK),
                Dispatch.REJECTED,
                node,
                "The link was lost and the transaction was rolled back; do it again.");
    }

    /** Whether the client failed because the connection did, not because the broker refused the call. */
    static boolean linkLevel(ActiveMQException e) {
        return e instanceof ActiveMQNotConnectedException
                || e instanceof ActiveMQObjectClosedException
                || e instanceof ActiveMQDisconnectedException
                || e instanceof ActiveMQConnectionTimedOutException
                || e instanceof ActiveMQUnBlockedException;
    }
}
