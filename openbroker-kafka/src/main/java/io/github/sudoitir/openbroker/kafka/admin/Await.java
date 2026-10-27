package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.time.Clock;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.KafkaFuture;

/** Waits for Admin futures within one call's deadline and reports their failures as the SDK's. */
public final class Await {

    private final Deadline deadline;
    private final Clock clock;
    private final NodeId node;

    Await(Deadline deadline, Clock clock, NodeId node) {
        this.deadline = deadline;
        this.clock = clock;
        this.node = node;
    }

    /** The time left, for the {@code timeoutMs} of an Admin call's options. */
    public int timeoutMs() {
        return (int) Math.clamp(deadline.remaining(clock).toMillis(), 1, Integer.MAX_VALUE);
    }

    public <T> T get(KafkaFuture<T> future) {
        try {
            return future.get(Math.max(1, deadline.remaining(clock).toMillis()), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw KafkaErrors.of(e.getCause() == null ? e : e.getCause(), node);
        } catch (TimeoutException e) {
            throw Deadlines.exceeded(node, Dispatch.IN_DOUBT);
        } catch (InterruptedException e) {
            throw Deadlines.cancelled(node, e);
        }
    }
}
