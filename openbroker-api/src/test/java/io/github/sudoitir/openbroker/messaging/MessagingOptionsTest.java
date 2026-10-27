package io.github.sudoitir.openbroker.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.ConnectOptions;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class MessagingOptionsTest {

    @Test
    void backoffGrowsByTheMultiplierAndStopsAtMax() {
        Backoff backoff = new Backoff(Duration.ofMillis(100), 2.0, Duration.ofSeconds(1), 0.2);
        assertThat(backoff.delay(1)).isEqualTo(Duration.ofMillis(100));
        assertThat(backoff.delay(2)).isEqualTo(Duration.ofMillis(200));
        assertThat(backoff.delay(3)).isEqualTo(Duration.ofMillis(400));
        assertThat(backoff.delay(4)).isEqualTo(Duration.ofMillis(800));
        assertThat(backoff.delay(5)).isEqualTo(Duration.ofSeconds(1));
        assertThat(backoff.delay(Integer.MAX_VALUE)).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    void backoffDelayNeedsAPositiveAttempt() {
        assertThatThrownBy(() -> Backoff.defaults().delay(0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void backoffIsValidated() {
        Duration one = Duration.ofSeconds(1);
        assertThatThrownBy(() -> new Backoff(Duration.ZERO, 2, one, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Backoff(one, 0.5, one, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Backoff(one, Double.NaN, one, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Backoff(one, 2, Duration.ofMillis(1), 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Backoff(one, 2, one, 1.5)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Backoff(one, 2, one, -0.1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Backoff(null, 2, one, 0)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void theDefaultsAreWhatTheyAreDocumentedToBe() {
        assertThat(Backoff.defaults()).isEqualTo(new Backoff(Duration.ofMillis(100), 2.0, Duration.ofSeconds(10), 0.2));
        assertThat(Recovery.defaults().giveUpAfter()).isEmpty();
        assertThat(SendBuffer.defaults()).isEqualTo(new SendBuffer(1000, 16L * 1024 * 1024, Duration.ofSeconds(30)));
        assertThat(FlowControl.defaults()).isEqualTo(new FlowControl(100, 100, 1024 * 1024));
        assertThat(FlowControl.none()).isEqualTo(new FlowControl(1, 0, 0));
        SenderOptions sender = SenderOptions.defaults();
        assertThat(sender.guarantee()).isEqualTo(Guarantee.AT_LEAST_ONCE);
        assertThat(sender.deduplication()).isEqualTo(DeduplicationScope.NONE);
        assertThat(sender.ordering()).isEqualTo(OrderingScope.NONE);
        ReceiverOptions receiver = ReceiverOptions.consume();
        assertThat(receiver.mode()).isEqualTo(ReceiveMode.CONSUME);
        assertThat(receiver.guarantee()).isEqualTo(Guarantee.AT_LEAST_ONCE);
        assertThat(receiver.flow()).isEqualTo(FlowControl.defaults());
        ProcessorOptions processor = ProcessorOptions.defaults();
        assertThat(processor.concurrency()).isEqualTo(8);
        assertThat(processor.ordering()).isEqualTo(OrderingScope.NONE);
        assertThat(processor.retry()).isEqualTo(RetryPolicy.defaults());
        assertThat(processor.retry().mode()).isEqualTo(RetryMode.DEFAULT);
        assertThat(processor.retry().maxAttempts()).isEqualTo(10);
        assertThat(processor.stopGrace()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void withMethodsChangeOnlyTheirField() {
        SenderOptions sender = SenderOptions.defaults()
                .withGuarantee(Guarantee.TRANSACTED)
                .withDeduplication(DeduplicationScope.WINDOW)
                .withOrdering(OrderingScope.GROUP);
        assertThat(sender.guarantee()).isEqualTo(Guarantee.TRANSACTED);
        assertThat(sender.deduplication()).isEqualTo(DeduplicationScope.WINDOW);
        assertThat(sender.ordering()).isEqualTo(OrderingScope.GROUP);
        assertThat(sender.buffer()).isEqualTo(SendBuffer.defaults());

        ReceiverOptions receiver = ReceiverOptions.consume().withFlow(FlowControl.none());
        assertThat(receiver.flow()).isEqualTo(FlowControl.none());
        assertThat(receiver.guarantee()).isEqualTo(Guarantee.AT_LEAST_ONCE);
        assertThat(ReceiverOptions.browse(Optional.empty()).mode()).isEqualTo(ReceiveMode.BROWSE);
    }

    @Test
    void bufferAndFlowAreValidated() {
        Duration one = Duration.ofSeconds(1);
        assertThatThrownBy(() -> new SendBuffer(0, 1, one)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SendBuffer(1, 0, one)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SendBuffer(1, 1, Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FlowControl(0, 0, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FlowControl(1, -1, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FlowControl(1, 0, -1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void concurrencyIsOneToSixtyFour() {
        ProcessorOptions defaults = ProcessorOptions.defaults();
        assertThat(defaults.withConcurrency(1).concurrency()).isEqualTo(1);
        assertThat(defaults.withConcurrency(64).concurrency()).isEqualTo(64);
        assertThatThrownBy(() -> defaults.withConcurrency(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> defaults.withConcurrency(65)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void subscriptionOrderingNeedsOneLaneAndAProcessorConsumes() {
        ProcessorOptions defaults = ProcessorOptions.defaults();
        assertThatThrownBy(() -> defaults.withOrdering(OrderingScope.SUBSCRIPTION))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(defaults.withConcurrency(1)
                        .withOrdering(OrderingScope.SUBSCRIPTION)
                        .ordering())
                .isEqualTo(OrderingScope.SUBSCRIPTION);
        assertThatThrownBy(() -> new ProcessorOptions(
                        ReceiverOptions.browse(Optional.empty()),
                        1,
                        OrderingScope.NONE,
                        RetryPolicy.system(),
                        Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void retryPolicyIsValidated() {
        assertThat(RetryPolicy.system().mode()).isEqualTo(RetryMode.SYSTEM);
        RetryPolicy managed = RetryPolicy.managed(3, Backoff.defaults());
        assertThat(managed.mode()).isEqualTo(RetryMode.MANAGED);
        assertThat(managed.maxAttempts()).isEqualTo(3);
        assertThat(managed.deadLetter()).isEmpty();
        assertThatThrownBy(() -> RetryPolicy.managed(0, Backoff.defaults()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aDeduplicationWindowIsPositive() {
        assertThat(new DeduplicationWindow(OptionalLong.of(20_000), Optional.empty(), true).ids())
                .hasValue(20_000);
        assertThatThrownBy(() -> new DeduplicationWindow(OptionalLong.of(0), Optional.empty(), false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DeduplicationWindow(OptionalLong.empty(), Optional.of(Duration.ZERO), false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aPlainReceiptIsNotResentAndConnectionsDefaultToNoObserver() {
        assertThat(SendReceipt.accepted().resent()).isFalse();
        assertThat(ConnectOptions.defaults().observer()).isSameAs(MessagingObserver.none());
    }

    @Test
    void theNoOpObserverIsShared() {
        MessagingObserver none = MessagingObserver.none();
        Observation observation = none.onReceive(null);
        assertThat(none.onReceive(null)).isSameAs(observation);
        observation.end(null);
        observation.received(List.of());
    }
}
