package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.util.function.BooleanSupplier;

/**
 * Waits for a change to show. A Kafka broker serves reads from its own copy of the cluster's
 * metadata, which trails the controller by a moment and trails it differently on each broker, so a
 * list that follows a write on another broker can still show the old state. An operator who changes
 * something and lists it expects to see the change, so a write that returns waits until a check holds
 * on several reads in a row, which come from different brokers. Giving up at the deadline
 * is not a failure: the write was made.
 */
final class Settle {

    /**
     * How many reads in a row must agree. Each read goes to a broker the client picks, so with three
     * brokers five reads in a row miss one that is still behind about one time in eight.
     */
    private static final int ROUNDS = 5;

    private static final long PAUSE_MILLIS = 50;

    private Settle() {}

    static void until(Context context, BooleanSupplier shown) {
        int agreed = 0;
        while (agreed < ROUNDS) {
            if (holds(shown)) {
                agreed++;
                continue;
            }
            agreed = 0;
            if (context.await().timeoutMs() <= PAUSE_MILLIS) {
                return;
            }
            try {
                Thread.sleep(PAUSE_MILLIS);
            } catch (InterruptedException interrupted) {
                throw Deadlines.cancelled(KafkaCluster.CLUSTER, interrupted);
            }
        }
    }

    private static boolean holds(BooleanSupplier shown) {
        try {
            return shown.getAsBoolean();
        } catch (BrokerException e) {
            // A broker that has not heard of the change yet may say the thing does not exist.
            if (e.status() == Status.NOT_FOUND || e.status() == Status.UNAVAILABLE) {
                return false;
            }
            throw e;
        }
    }
}
