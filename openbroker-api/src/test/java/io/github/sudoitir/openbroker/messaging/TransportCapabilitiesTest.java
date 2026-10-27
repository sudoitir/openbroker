package io.github.sudoitir.openbroker.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.model.KindId;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TransportCapabilitiesTest {

    private static final KindId GROUP = KindId.of("x.group");
    private static final KindId OTHER = KindId.of("x.other");

    private static final SubscriptionCapabilities GROUP_CAPS = new SubscriptionCapabilities(
            Set.of(OrderingScope.PARTITION), SettlementScope.MESSAGE, false, SystemRetry.NONE);

    private static final TransportCapabilities CAPS = new TransportCapabilities(
            Set.of(Guarantee.AT_LEAST_ONCE),
            DeduplicationScope.SESSION,
            Set.of(OrderingScope.NONE),
            Set.of(PrefetchBound.BYTES),
            Map.of(GROUP, GROUP_CAPS),
            false);

    @Test
    void aKindTheTransportOffersHasItsOwnCapabilities() {
        assertThat(CAPS.subscription(GROUP)).isSameAs(GROUP_CAPS);
        assertThat(CAPS.subscription(GROUP).retry()).isEqualTo(SystemRetry.NONE);
    }

    @Test
    void aKindTheTransportDoesNotOfferIsRefused() {
        assertThatThrownBy(() -> CAPS.subscription(OTHER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("x.other");
    }
}
