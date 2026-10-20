package io.github.sudoitir.openbroker.support;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.model.MetricValue;
import io.github.sudoitir.openbroker.model.Precision;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class CounterRateTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void dividesTheIncreaseByTheElapsedSeconds() {
        assertThat(CounterRate.perSecond(MetricValue.exact(100), T0, MetricValue.exact(160), T0.plusSeconds(30)))
                .hasValue(2.0);
    }

    @Test
    void aCounterThatWentBackwardsHasNoRate() {
        assertThat(CounterRate.perSecond(MetricValue.exact(100), T0, MetricValue.exact(5), T0.plusSeconds(30)))
                .isEmpty();
    }

    @Test
    void aMovedStartIsARestartAndHasNoRate() {
        MetricValue before = new MetricValue(100, Precision.EXACT, Optional.of(T0.minusSeconds(60)), Map.of());
        MetricValue after = new MetricValue(200, Precision.EXACT, Optional.of(T0.plusSeconds(10)), Map.of());
        assertThat(CounterRate.perSecond(before, T0, after, T0.plusSeconds(30))).isEmpty();
    }

    @Test
    void noElapsedTimeHasNoRate() {
        assertThat(CounterRate.perSecond(MetricValue.exact(1), T0, MetricValue.exact(2), T0))
                .isEmpty();
    }
}
