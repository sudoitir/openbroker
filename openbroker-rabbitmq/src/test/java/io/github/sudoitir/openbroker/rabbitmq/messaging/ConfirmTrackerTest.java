package io.github.sudoitir.openbroker.rabbitmq.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class ConfirmTrackerTest {

    private final ConfirmTracker tracker =
            new ConfirmTracker(Optional.of(ResourceId.of("rabbitmq.exchange", "/", "x")));
    private final CompletableFuture<SendReceipt> one = new CompletableFuture<>();
    private final CompletableFuture<SendReceipt> two = new CompletableFuture<>();
    private final CompletableFuture<SendReceipt> three = new CompletableFuture<>();

    private void publishAll() {
        tracker.published(1, one);
        tracker.published(2, "k", two, SendReceipt.accepted());
        tracker.published(3, "k", three, SendReceipt.accepted());
    }

    private static BrokerException failure(CompletableFuture<SendReceipt> future) {
        assertThat(future).isCompletedExceptionally();
        return (BrokerException) future.handle((r, e) -> e).join();
    }

    @Test
    void anAckConfirmsOnlyItsSequence() {
        publishAll();
        tracker.ack(2, false);
        assertThat(two).isCompleted();
        assertThat(one).isNotDone();
        assertThat(tracker.unconfirmed()).isEqualTo(2);
    }

    @Test
    void aMultipleAckConfirmsEverythingUpToIt() {
        publishAll();
        tracker.ack(2, true);
        assertThat(one).isCompleted();
        assertThat(two).isCompleted();
        assertThat(three).isNotDone();
    }

    @Test
    void anUnknownAckIsIgnored() {
        publishAll();
        tracker.ack(9, false);
        assertThat(tracker.unconfirmed()).isEqualTo(3);
    }

    @Test
    void aNackFailsDestinationFull() {
        publishAll();
        tracker.nack(1, false);
        BrokerException e = failure(one);
        assertThat(e.status()).isEqualTo(Status.RESOURCE_EXHAUSTED);
        assertThat(e.reason()).isEqualTo(ErrorReasons.DESTINATION_FULL);
        assertThat(two).isNotDone();
    }

    @Test
    void aMultipleNackFailsEverythingUpToIt() {
        publishAll();
        tracker.nack(2, true);
        assertThat(failure(one).reason()).isEqualTo(ErrorReasons.DESTINATION_FULL);
        assertThat(failure(two).reason()).isEqualTo(ErrorReasons.DESTINATION_FULL);
        assertThat(three).isNotDone();
    }

    @Test
    void aReturnBeforeTheAckFailsNotRouted() {
        publishAll();
        assertThat(tracker.returned("k", 312, "NO_ROUTE")).isTrue();
        assertThat(two).isNotDone();
        tracker.ack(3, true);
        BrokerException e = failure(two);
        assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
        assertThat(e.reason()).isEqualTo(ErrorReasons.NOT_ROUTED);
        assertThat(e.dispatch()).isEqualTo(Dispatch.REJECTED);
        assertThat(one).isCompleted();
        assertThat(three).isCompleted();
    }

    @Test
    void aSecondReturnUnderTheSameKeyTakesTheNextPublish() {
        publishAll();
        tracker.returned("k", 312, "NO_ROUTE");
        tracker.returned("k", 312, "NO_ROUTE");
        tracker.ack(3, true);
        assertThat(failure(two).reason()).isEqualTo(ErrorReasons.NOT_ROUTED);
        assertThat(failure(three).reason()).isEqualTo(ErrorReasons.NOT_ROUTED);
    }

    @Test
    void aReturnedPublishThatIsNackedStaysNotRouted() {
        publishAll();
        tracker.returned(2, 312, "NO_ROUTE");
        tracker.nack(2, false);
        assertThat(failure(two).reason()).isEqualTo(ErrorReasons.NOT_ROUTED);
    }

    @Test
    void aReturnOfNothingPendingFindsNothing() {
        publishAll();
        assertThat(tracker.returned("other", 312, "NO_ROUTE")).isFalse();
        tracker.returned(42, 312, "NO_ROUTE");
        tracker.ack(3, true);
        assertThat(one).isCompleted();
    }

    @Test
    void aPublishThatWasNeverWrittenCanBeWithdrawn() {
        publishAll();
        assertThat(tracker.withdraw(1)).isTrue();
        assertThat(tracker.withdraw(1)).isFalse();
        assertThat(tracker.unconfirmed()).isEqualTo(2);
    }

    @Test
    void aLostChannelLeavesTheUnconfirmedInDoubt() {
        publishAll();
        tracker.ack(1, false);
        tracker.channelLost();
        BrokerException e = failure(two);
        assertThat(ConfirmTracker.isLoss(e)).isTrue();
        assertThat(e.dispatch()).isEqualTo(Dispatch.IN_DOUBT);
        assertThat(e.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(failure(three)).isSameAs(e);
        assertThat(one).isCompleted();
        assertThat(tracker.unconfirmed()).isZero();
    }

    @Test
    void anOrdinaryFailureIsNotALoss() {
        assertThat(ConfirmTracker.isLoss(new IllegalStateException())).isFalse();
        ConfirmTracker bare = new ConfirmTracker();
        CompletableFuture<SendReceipt> f = new CompletableFuture<>();
        bare.published(1, f);
        bare.ack(1, false);
        assertThat(f).isCompletedWithValue(SendReceipt.accepted());
    }
}
