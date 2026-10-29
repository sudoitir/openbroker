package io.github.sudoitir.openbroker.kafka.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Which bootstrap servers can be reached, checked before a Kafka client is made for them. A client given
 * a name that does not resolve fails to construct, and one given a closed port retries until its
 * deadline, so neither says what is wrong; here each address is resolved and connected to, and the
 * client is given only those that answer. When none does, the call fails at once, naming the cause of
 * the first and listing the rest.
 */
final class Bootstrap {

    private Bootstrap() {}

    /** The servers that accept a connection, in order; fails when none does. */
    static List<String> reachable(List<String> servers, Duration budget, Clock clock) {
        Deadline limit = Deadline.after(budget, clock);
        Map<String, Future<Optional<Sockets.Failure>>> running = new LinkedHashMap<>();
        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        List<String> ok = new ArrayList<>();
        Map<String, Sockets.Failure> failed = new LinkedHashMap<>();
        try {
            for (String server : servers) {
                running.put(server, pool.submit(() -> Sockets.open(server, limit, clock)));
            }
            long give = System.nanoTime() + budget.plusMillis(250).toNanos();
            for (Map.Entry<String, Future<Optional<Sockets.Failure>>> e : running.entrySet()) {
                Optional<Sockets.Failure> failure = await(e.getValue(), Math.max(0, give - System.nanoTime()));
                if (failure.isEmpty()) {
                    ok.add(e.getKey());
                } else {
                    failed.put(e.getKey(), failure.get());
                }
            }
        } finally {
            // A name lookup cannot be interrupted, so the pool is not waited for.
            pool.shutdownNow();
        }
        if (!ok.isEmpty()) {
            return ok;
        }
        throw unreachable(failed);
    }

    private static Optional<Sockets.Failure> await(Future<Optional<Sockets.Failure>> future, long nanos) {
        try {
            return future.get(nanos, TimeUnit.NANOSECONDS);
        } catch (TimeoutException _) {
            future.cancel(true);
            return Optional.of(Sockets.Failure.TIMEOUT);
        } catch (ExecutionException _) {
            return Optional.of(Sockets.Failure.TIMEOUT);
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            return Optional.of(Sockets.Failure.TIMEOUT);
        }
    }

    private static String describe(String server, Sockets.Failure failure) {
        return switch (failure) {
            case DNS -> server + " does not resolve";
            case REFUSED -> server + " refuses connections";
            case TIMEOUT -> server + " did not accept a connection in time";
        };
    }

    /** The failure naming the first server's cause in the metadata and every server in the message. */
    static BrokerException unreachable(Map<String, Sockets.Failure> failed) {
        Map.Entry<String, Sockets.Failure> first = failed.entrySet().iterator().next();
        List<String> parts = new ArrayList<>();
        failed.forEach((server, failure) -> parts.add(describe(server, failure)));
        return new BrokerException(
                Status.UNAVAILABLE,
                ErrorInfo.of(
                        ErrorReasons.UNREACHABLE,
                        Map.of(
                                Reach.ENDPOINT,
                                first.getKey(),
                                Reach.FAILURE,
                                first.getValue().text())),
                Dispatch.NOT_SENT,
                null,
                String.join("; ", parts) + ".");
    }
}
