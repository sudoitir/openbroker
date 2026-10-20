package io.github.sudoitir.openbroker.artemis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** A bounded selector keeps its bound: a filter that would escape it is refused. */
class ArtemisFiltersTest {

    private static final Instant T0 = Instant.ofEpochMilli(1_000);

    @Test
    void aFilterIsAndedWithTheBound() {
        assertThat(ArtemisFilters.producedAtOrBefore(Optional.of(ArtemisFilters.selector("a = 1")), T0)
                        .expression())
                .isEqualTo("(a = 1) AND AMQTimestamp <= 1000");
        assertThat(ArtemisFilters.producedAtOrBefore(Optional.empty(), T0).expression())
                .isEqualTo("AMQTimestamp <= 1000");
    }

    @Test
    void aFilterThatClosesItsOwnParenthesisToEscapeTheBoundIsRefused() {
        assertThatThrownBy(() ->
                        ArtemisFilters.producedAtOrBefore(Optional.of(ArtemisFilters.selector("a=1) OR (1=1")), T0))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.reason()).isEqualTo(ErrorReasons.INVALID_FILTER));
    }
}
