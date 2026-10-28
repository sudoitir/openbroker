package io.github.sudoitir.openbroker.artemis.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.artemis.ArtemisKinds;
import io.github.sudoitir.openbroker.messaging.DeduplicationScope;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.OrderingScope;
import io.github.sudoitir.openbroker.messaging.PrefetchBound;
import io.github.sudoitir.openbroker.messaging.SettlementScope;
import io.github.sudoitir.openbroker.messaging.SubscriptionCapabilities;
import io.github.sudoitir.openbroker.messaging.SystemRetry;
import io.github.sudoitir.openbroker.messaging.TransportCapabilities;
import org.junit.jupiter.api.Test;

/** What the Artemis transport declares; the TCK holds it to every word. */
class ArtemisTransportCapabilitiesTest {

    private static final TransportCapabilities CAPABILITIES = ArtemisTransport.CAPABILITIES;
    private static final SubscriptionCapabilities QUEUE = CAPABILITIES.subscription(ArtemisKinds.QUEUE);

    @Test
    void itOffersEveryGuaranteeAndDeduplicatesById() {
        assertThat(CAPABILITIES.guarantees()).containsExactlyInAnyOrder(Guarantee.values());
        assertThat(CAPABILITIES.deduplication()).isEqualTo(DeduplicationScope.WINDOW);
    }

    @Test
    void itKeepsGroupsAndTheWholeSubscriptionInOrderButHasNoPartitions() {
        assertThat(CAPABILITIES.orderings())
                .containsExactlyInAnyOrder(OrderingScope.NONE, OrderingScope.GROUP, OrderingScope.SUBSCRIPTION);
        assertThat(QUEUE.orderings())
                .containsExactlyInAnyOrder(OrderingScope.NONE, OrderingScope.GROUP, OrderingScope.SUBSCRIPTION);
    }

    @Test
    void itBoundsReadAheadInBytesAndReleasesTheWholeReceiver() {
        assertThat(CAPABILITIES.prefetchBounds()).containsExactly(PrefetchBound.BYTES);
        assertThat(QUEUE.pullSettlement()).isEqualTo(SettlementScope.RECEIVER);
    }

    @Test
    void theBrokerDelaysCountsAndDeadLettersUnderASystemRetry() {
        assertThat(QUEUE.retry()).isEqualTo(new SystemRetry(true, true, true));
    }

    @Test
    void aReleaseThatIsNotAFailedDeliveryIsNotCounted() {
        assertThat(QUEUE.releaseCountsDelivery()).isFalse();
    }

    @Test
    void itOffersOneSubscriptionKindAndRefusesAskingAboutAnother() {
        assertThat(CAPABILITIES.subscriptions()).containsOnlyKeys(ArtemisKinds.QUEUE);
        assertThatThrownBy(() -> CAPABILITIES.subscription(ArtemisKinds.ADDRESS))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void itJoinsSendersToTransactionsAndFollowsAFailover() {
        assertThat(QUEUE.joinedTransactions()).isTrue();
        assertThat(CAPABILITIES.failover()).isTrue();
    }
}
