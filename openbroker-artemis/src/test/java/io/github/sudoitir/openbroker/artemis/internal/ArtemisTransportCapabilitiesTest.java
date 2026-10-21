package io.github.sudoitir.openbroker.artemis.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.messaging.Enforcement;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.OrderingScope;
import io.github.sudoitir.openbroker.messaging.PrefetchBound;
import io.github.sudoitir.openbroker.messaging.SettlementScope;
import io.github.sudoitir.openbroker.messaging.TransportCapabilities;
import org.junit.jupiter.api.Test;

/** What the Artemis transport declares; the TCK holds it to every word. */
class ArtemisTransportCapabilitiesTest {

    private static final TransportCapabilities CAPABILITIES = ArtemisTransport.CAPABILITIES;

    @Test
    void itOffersEveryGuaranteeAndDeduplicatesById() {
        assertThat(CAPABILITIES.guarantees()).containsExactlyInAnyOrder(Guarantee.values());
        assertThat(CAPABILITIES.deduplication()).isTrue();
    }

    @Test
    void itKeepsGroupsAndTheWholeSubscriptionInOrderButHasNoPartitions() {
        assertThat(CAPABILITIES.orderings())
                .containsExactlyInAnyOrder(OrderingScope.NONE, OrderingScope.GROUP, OrderingScope.SUBSCRIPTION);
    }

    @Test
    void itBoundsReadAheadInBytesAndReleasesTheWholeReceiver() {
        assertThat(CAPABILITIES.prefetchBounds()).containsExactly(PrefetchBound.BYTES);
        assertThat(CAPABILITIES.pullSettlement()).isEqualTo(SettlementScope.RECEIVER);
    }

    @Test
    void aManagedRetryIsTheClientsWhileTheBrokerOwnsASystemOne() {
        assertThat(CAPABILITIES.retry().delay()).isEqualTo(Enforcement.CLIENT);
        assertThat(CAPABILITIES.retry().attempts()).isEqualTo(Enforcement.CLIENT);
        assertThat(CAPABILITIES.retry().deadLetter()).isEqualTo(Enforcement.CLIENT);
    }

    @Test
    void itJoinsSendersToTransactionsAndFollowsAFailover() {
        assertThat(CAPABILITIES.joinedTransactions()).isTrue();
        assertThat(CAPABILITIES.failover()).isTrue();
    }
}
