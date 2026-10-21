package io.github.sudoitir.openbroker.artemis.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.artemis.internal.Settlements.Action;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** What each disposition does on Artemis, for each way a receiver holds messages. */
class SettlementsTest {

    private static final Disposition ACCEPT = Disposition.accept();
    private static final Disposition RELEASE = new Disposition.Release(Optional.empty(), false);
    private static final Disposition RELEASE_COUNTED = new Disposition.Release(Optional.empty(), true);
    private static final Disposition DELAYED = new Disposition.Release(Optional.of(Duration.ofSeconds(1)), true);
    private static final Disposition REJECT = new Disposition.Reject(Optional.of("bad"));

    private static Action alo(Disposition d) {
        return Settlements.of(d, Guarantee.AT_LEAST_ONCE, false, false);
    }

    @Test
    void atLeastOnceAcknowledgesAcceptsAndHandsBackReleases() {
        assertThat(alo(ACCEPT)).isEqualTo(Action.ACKNOWLEDGE);
        assertThat(alo(RELEASE)).isEqualTo(Action.ROLLBACK);
        assertThat(alo(RELEASE_COUNTED)).isEqualTo(Action.ROLLBACK_COUNTED);
    }

    @Test
    void aReleaseWithADelayIsACopyAndARejectionIsDeadLetteredNow() {
        assertThat(alo(DELAYED)).isEqualTo(Action.COPY_AFTER_DELAY);
        assertThat(alo(REJECT)).isEqualTo(Action.DEAD_LETTER);
    }

    @Test
    void aMessageFromBeforeTheLinkRecoveredIsStale() {
        for (Disposition d : new Disposition[] {ACCEPT, RELEASE, DELAYED, REJECT}) {
            assertThat(Settlements.of(d, Guarantee.AT_LEAST_ONCE, false, true)).isEqualTo(Action.STALE);
            assertThat(Settlements.of(d, Guarantee.TRANSACTED, false, true)).isEqualTo(Action.STALE);
        }
    }

    @Test
    void atMostOnceHasAlreadySettledEverythingAndRefusesTheRest() {
        assertThat(Settlements.of(ACCEPT, Guarantee.AT_MOST_ONCE, false, false)).isEqualTo(Action.FORGET);
        assertThat(Settlements.of(ACCEPT, Guarantee.AT_MOST_ONCE, false, true)).isEqualTo(Action.FORGET);
        assertThat(Settlements.of(RELEASE, Guarantee.AT_MOST_ONCE, false, false))
                .isEqualTo(Action.REFUSED);
        assertThat(Settlements.of(REJECT, Guarantee.AT_MOST_ONCE, false, false)).isEqualTo(Action.REFUSED);
    }

    @Test
    void aTransactionDecidesWhatHappensToAReleaseOrARejection() {
        assertThat(Settlements.of(ACCEPT, Guarantee.TRANSACTED, false, false)).isEqualTo(Action.ACKNOWLEDGE);
        assertThat(Settlements.of(RELEASE, Guarantee.TRANSACTED, false, false)).isEqualTo(Action.REFUSED);
        assertThat(Settlements.of(DELAYED, Guarantee.TRANSACTED, false, false)).isEqualTo(Action.REFUSED);
        assertThat(Settlements.of(REJECT, Guarantee.TRANSACTED, false, false)).isEqualTo(Action.REFUSED);
    }

    @Test
    void aBrowserLeavesEveryMessageWhereItIs() {
        for (Disposition d : new Disposition[] {ACCEPT, RELEASE, REJECT}) {
            assertThat(Settlements.of(d, Guarantee.AT_LEAST_ONCE, true, false)).isEqualTo(Action.FORGET);
        }
    }
}
