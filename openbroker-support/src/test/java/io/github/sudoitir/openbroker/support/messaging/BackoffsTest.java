package io.github.sudoitir.openbroker.support.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.messaging.Backoff;
import java.time.Duration;
import java.util.random.RandomGenerator;
import org.junit.jupiter.api.Test;

class BackoffsTest {

    private static final Backoff PLAIN = new Backoff(Duration.ofMillis(100), 2.0, Duration.ofSeconds(1), 0.0);
    private static final Backoff JITTERED = new Backoff(Duration.ofMillis(100), 2.0, Duration.ofMillis(300), 0.5);

    @Test
    void withoutJitterIsTheBackoffsOwnDelay() {
        assertThat(Backoffs.delay(PLAIN, 3, () -> 0L)).isEqualTo(Duration.ofMillis(400));
    }

    @Test
    void jitterSpreadsToBothSides() {
        RandomGenerator low = () -> 0L;
        RandomGenerator middle = () -> Long.MIN_VALUE;
        assertThat(Backoffs.delay(JITTERED, 1, low)).isEqualTo(Duration.ofMillis(50));
        assertThat(Backoffs.delay(JITTERED, 1, middle)).isEqualTo(Duration.ofMillis(100));
    }

    @Test
    void neverExceedsTheMaximum() {
        RandomGenerator high = () -> -1L;
        assertThat(Backoffs.delay(JITTERED, 2, high)).isEqualTo(Duration.ofMillis(300));
        assertThat(Backoffs.delay(JITTERED, 9, high)).isEqualTo(Duration.ofMillis(300));
    }
}
