package io.github.sudoitir.openbroker.support.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Backoff;
import io.github.sudoitir.openbroker.messaging.OrderingScope;
import io.github.sudoitir.openbroker.messaging.RetryMode;
import io.github.sudoitir.openbroker.messaging.RetryPolicy;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.SystemRetry;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RetriesTest {

    private static final SystemRetry COUNTING = new SystemRetry(true, true, true);
    private static final SystemRetry COUNTING_ONLY = new SystemRetry(false, true, false);

    @Test
    void theDefaultBecomesTheSystemsPolicyWhereTheSystemCountsAttempts() {
        RetryPolicy resolved = Retries.resolve(RetryPolicy.defaults(), COUNTING, OrderingScope.NONE)
                .policy();
        assertThat(resolved.mode()).isEqualTo(RetryMode.SYSTEM);
        assertThat(Retries.resolve(RetryPolicy.defaults(), COUNTING_ONLY, OrderingScope.NONE)
                        .policy()
                        .mode())
                .isEqualTo(RetryMode.SYSTEM);
    }

    @Test
    void theDefaultBecomesTenManagedAttemptsWhereTheSystemCountsNone() {
        RetryPolicy resolved = Retries.resolve(RetryPolicy.defaults(), SystemRetry.NONE, OrderingScope.NONE)
                .policy();
        assertThat(resolved.mode()).isEqualTo(RetryMode.MANAGED);
        assertThat(resolved.maxAttempts()).isEqualTo(10);
        assertThat(resolved.backoff()).isEqualTo(Backoff.defaults());
        assertThat(resolved.deadLetter()).isEmpty();
    }

    @Test
    void aSystemThatDelaysAndDeadLettersButCountsNothingStillGetsAManagedDefault() {
        assertThat(Retries.resolve(RetryPolicy.defaults(), new SystemRetry(true, false, true), OrderingScope.NONE)
                        .policy()
                        .mode())
                .isEqualTo(RetryMode.MANAGED);
    }

    @Test
    void anExplicitSystemPolicyIsKeptWhereTheSystemCountsAttempts() {
        assertThat(Retries.resolve(RetryPolicy.system(), COUNTING, OrderingScope.NONE)
                        .policy())
                .isEqualTo(RetryPolicy.system());
    }

    @Test
    void anExplicitSystemPolicyWhereNothingIsCountedIsUnimplementedAndNotSent() {
        assertThatThrownBy(() -> Retries.resolve(RetryPolicy.system(), SystemRetry.NONE, OrderingScope.NONE))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED);
                    assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
                    assertThat(e.getMessage()).contains("SYSTEM");
                });
    }

    @Test
    void aManagedPolicyIsAlwaysKeptAndMayNameADeadLetterTarget() {
        SendTarget dead = SendTarget.subscription(new ResourceId(KindId.of("x.queue"), ResourcePath.of("dead")));
        RetryPolicy managed = RetryPolicy.managed(3, Backoff.defaults()).withDeadLetter(dead);
        assertThat(Retries.resolve(managed, SystemRetry.NONE, OrderingScope.NONE)
                        .policy())
                .isEqualTo(managed);
        assertThat(Retries.resolve(managed, COUNTING, OrderingScope.NONE).policy())
                .isEqualTo(managed);
    }

    @Test
    void aDefaultThatBecomesManagedKeepsItsDeadLetterTarget() {
        SendTarget dead = SendTarget.subscription(new ResourceId(KindId.of("x.queue"), ResourcePath.of("dead")));
        RetryPolicy asked = new RetryPolicy(RetryMode.DEFAULT, 4, Backoff.defaults(), Optional.of(dead));
        RetryPolicy resolved =
                Retries.resolve(asked, SystemRetry.NONE, OrderingScope.NONE).policy();
        assertThat(resolved.mode()).isEqualTo(RetryMode.MANAGED);
        assertThat(resolved.maxAttempts()).isEqualTo(4);
        assertThat(resolved.deadLetter()).contains(dead);
    }

    @Test
    void theResolvedDefaultRetriesInPlaceWhateverTheOrdering() {
        assertThat(Retries.resolve(RetryPolicy.defaults(), SystemRetry.NONE, OrderingScope.NONE)
                        .inPlace())
                .isTrue();
        assertThat(Retries.resolve(RetryPolicy.defaults(), SystemRetry.NONE, OrderingScope.GROUP)
                        .inPlace())
                .isTrue();
    }

    @Test
    void anExplicitManagedPolicyRetriesInPlaceOnlyWhenOrdered() {
        RetryPolicy managed = RetryPolicy.managed(3, Backoff.defaults());
        assertThat(Retries.resolve(managed, SystemRetry.NONE, OrderingScope.NONE)
                        .inPlace())
                .isFalse();
        assertThat(Retries.resolve(managed, SystemRetry.NONE, OrderingScope.PARTITION)
                        .inPlace())
                .isTrue();
    }
}
