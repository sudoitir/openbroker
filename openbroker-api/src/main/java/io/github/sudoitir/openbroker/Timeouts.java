package io.github.sudoitir.openbroker;

import java.time.Duration;
import java.util.Objects;

/**
 * How long establishing a connection, and a call made outside a {@link CallScope}, may take. Read by
 * a driver each time it builds a transport or makes a call, so a caller whose settings change at run
 * time passes a live view and connections follow it without reconnecting.
 */
public interface Timeouts {

    Duration connect();

    Duration call();

    static Timeouts fixed(Duration connect, Duration call) {
        Objects.requireNonNull(connect, "connect");
        Objects.requireNonNull(call, "call");
        return new Timeouts() {
            @Override
            public Duration connect() {
                return connect;
            }

            @Override
            public Duration call() {
                return call;
            }
        };
    }
}
