package io.github.sudoitir.openbroker.kafka.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.messaging.OrderingScope;
import io.github.sudoitir.openbroker.messaging.SettlementScope;
import io.github.sudoitir.openbroker.messaging.SubscriptionCapabilities;
import io.github.sudoitir.openbroker.messaging.SystemRetry;
import java.util.Set;
import org.junit.jupiter.api.Test;

class KafkaTransportCapabilitiesTest {

    @Test
    void aConsumerGroupCountsOnlyFailedDeliveriesAndJoinsTransactions() {
        SubscriptionCapabilities group = KafkaTransport.consumerGroup(Set.of(OrderingScope.PARTITION));

        assertThat(group.releaseCountsDelivery()).isFalse();
        assertThat(group.joinedTransactions()).isTrue();
        assertThat(group.pullSettlement()).isEqualTo(SettlementScope.MESSAGE);
        assertThat(group.retry()).isEqualTo(SystemRetry.NONE);
    }

    @Test
    void aShareGroupCountsEveryRedeliveryAndDeadLettersOnlyFromShareVersionTwo() {
        SubscriptionCapabilities v1 = KafkaTransport.shareGroup(new ShareFeatures(1, 2, true));
        SubscriptionCapabilities v2 = KafkaTransport.shareGroup(new ShareFeatures(2, 2, true));

        assertThat(v1.releaseCountsDelivery()).isTrue();
        assertThat(v2.releaseCountsDelivery()).isTrue();
        assertThat(v1.joinedTransactions()).isFalse();
        assertThat(v1.retry()).isEqualTo(new SystemRetry(false, true, false));
        assertThat(v2.retry()).isEqualTo(new SystemRetry(false, true, true));
    }
}
