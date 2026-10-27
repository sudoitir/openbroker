package io.github.sudoitir.openbroker.kafka.messaging;

import java.time.Duration;
import java.util.Map;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

/**
 * Tells whether the group coordinator still answers a member. The client retries a lost connection
 * quietly and its poll never fails, so the answers to its heartbeats are the only sign that the
 * cluster is out of reach. Brokers answer every few seconds. {@link #look} is called from the poll
 * thread, {@link #silent} from any thread.
 */
final class CoordinatorWatch {

    private static final Duration SILENT_AFTER = Duration.ofSeconds(15);
    private static final Duration LOOK_EVERY = Duration.ofSeconds(1);
    private static final String HEARTBEAT_ANSWERS = "heartbeat-total";

    private volatile long lastAnswer = System.nanoTime();
    private long lastCount;
    private long lastLook;

    /** Notes whether the coordinator answered a heartbeat since the last look. */
    void look(Map<MetricName, ? extends Metric> metrics) {
        long now = System.nanoTime();
        if (now - lastLook < LOOK_EVERY.toNanos()) {
            return;
        }
        lastLook = now;
        for (var metric : metrics.entrySet()) {
            if (metric.getKey().name().equals(HEARTBEAT_ANSWERS)
                    && metric.getValue().metricValue() instanceof Number count
                    && count.longValue() != lastCount) {
                lastCount = count.longValue();
                lastAnswer = now;
            }
        }
    }

    /** No heartbeat has been answered for a quarter of a minute. */
    boolean silent() {
        return System.nanoTime() - lastAnswer > SILENT_AFTER.toNanos();
    }
}
