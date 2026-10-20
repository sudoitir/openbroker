package io.github.sudoitir.openbroker.support;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.CallScope;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Status;
import org.jspecify.annotations.Nullable;

/** How a driver resolves a call's deadline and reports the end of one. */
public final class Deadlines {

    private Deadlines() {}

    /**
     * The deadline of the call entering now: the caller's {@link CallScope#DEADLINE} when bound, else
     * the connection's call timeout from now. Read it on the calling thread, then carry it.
     */
    public static Deadline current(ConnectOptions options) {
        return CallScope.deadline().orElseGet(() -> Deadline.after(options.callTimeout(), options.clock()));
    }

    /** The failure for a call that ran out of time. */
    public static BrokerException exceeded(@Nullable NodeId node, Dispatch dispatch) {
        return new BrokerException(
                Status.DEADLINE_EXCEEDED,
                ErrorInfo.of("DEADLINE_EXCEEDED"),
                dispatch,
                node,
                "The call's deadline passed");
    }

    /** The failure for a call interrupted by its caller; the thread's interrupt flag is restored. */
    public static BrokerException cancelled(@Nullable NodeId node, InterruptedException cause) {
        Thread.currentThread().interrupt();
        return new BrokerException(
                Status.CANCELLED, ErrorInfo.of("CANCELLED"), Dispatch.IN_DOUBT, node, "The call was cancelled", cause);
    }
}
