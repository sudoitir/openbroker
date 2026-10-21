package io.github.sudoitir.openbroker.support.messaging;

import java.time.Duration;

/** How the engine waits out a backoff; a test replaces it to run without real time. */
@FunctionalInterface
public interface Sleeper {

    void sleep(Duration duration) throws InterruptedException;

    /** Sleeps the calling thread. */
    static Sleeper system() {
        return Thread::sleep;
    }
}
