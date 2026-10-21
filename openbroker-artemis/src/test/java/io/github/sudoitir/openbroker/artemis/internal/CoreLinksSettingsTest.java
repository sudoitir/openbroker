package io.github.sudoitir.openbroker.artemis.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.Timeouts;
import io.github.sudoitir.openbroker.artemis.internal.CoreLinks.LinkSettings;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import java.time.Duration;
import java.util.Optional;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.junit.jupiter.api.Test;

/**
 * The Core connection settings a caller's timeouts turn into. They decide how a dead broker is noticed
 * and how much a consumer holds, so a change to them is a change in behaviour, not a refactoring.
 */
class CoreLinksSettingsTest {

    private static final ConnectOptions OPTIONS =
            ConnectOptions.defaults().withTimeouts(Timeouts.fixed(Duration.ofSeconds(2), Duration.ofSeconds(7)));
    private static final LinkSettings OPERATOR = LinkSettings.operator(true);

    private static ActiveMQConnectionFactory factory(LinkSettings settings) {
        return CoreLinks.connectionFactory(OPTIONS, "tcp://broker:61616", Optional.empty(), settings);
    }

    @Test
    void aConnectionNeverRetriesOnItsOwnAndTimesOutWithTheCaller() {
        ActiveMQConnectionFactory factory =
                CoreLinks.connectionFactory(OPTIONS, "tcp://broker:61616", Optional.empty(), OPERATOR);

        assertThat(factory.getInitialConnectAttempts()).isEqualTo(1);
        assertThat(factory.getReconnectAttempts()).isZero();
        assertThat(factory.getCallTimeout()).isEqualTo(7_000);
    }

    @Test
    void theBrokerDropsASilentConnectionOnlyAfterSeveralMissedPings() {
        ActiveMQConnectionFactory factory =
                CoreLinks.connectionFactory(OPTIONS, "tcp://broker:61616", Optional.empty(), OPERATOR);

        assertThat(factory.getClientFailureCheckPeriod()).isEqualTo(7_000);
        assertThat(factory.getConnectionTTL()).isEqualTo(21_000);
    }

    @Test
    void aConsumerHoldsAtMostOneWindowAndTheTopologyNeverRedirectsIt() {
        ActiveMQConnectionFactory factory =
                CoreLinks.connectionFactory(OPTIONS, "broker:61616", Optional.empty(), OPERATOR);

        assertThat(factory.getConsumerWindowSize()).isEqualTo(CoreLinks.WINDOW_BYTES);
        assertThat(factory.getServerLocator().getUseTopologyForLoadBalancing()).isFalse();
        assertThat(factory.getServerLocator()
                        .getStaticTransportConfigurations()[0]
                        .getParams())
                .containsEntry("host", "broker")
                .containsEntry(TransportConstants.NETTY_CONNECT_TIMEOUT, "2000");
    }

    @Test
    void anAtLeastOnceSenderAsksTheBrokerToConfirmEverySendWithoutWaitingForEach() {
        ActiveMQConnectionFactory factory = factory(LinkSettings.sender(Guarantee.AT_LEAST_ONCE));

        assertThat(factory.getConfirmationWindowSize()).isEqualTo(CoreLinks.CONFIRMATION_WINDOW_BYTES);
        assertThat(factory.isBlockOnDurableSend()).isFalse();
        assertThat(factory.isBlockOnNonDurableSend()).isFalse();
    }

    @Test
    void anAtMostOnceSenderAsksForNoConfirmationAndNeverWaits() {
        ActiveMQConnectionFactory factory = factory(LinkSettings.sender(Guarantee.AT_MOST_ONCE));

        assertThat(factory.getConfirmationWindowSize()).isEqualTo(-1);
        assertThat(factory.isBlockOnDurableSend()).isFalse();
        assertThat(factory.isBlockOnNonDurableSend()).isFalse();
    }

    @Test
    void anAtMostOnceReceiverIsAcknowledgedOnDeliveryAndBuffersWhatItWasGiven() {
        ActiveMQConnectionFactory factory = factory(LinkSettings.receiver(Guarantee.AT_MOST_ONCE, 4096));

        assertThat(factory.isPreAcknowledge()).isTrue();
        assertThat(factory.getConsumerWindowSize()).isEqualTo(4096);
    }

    @Test
    void anAtLeastOnceReceiverDoesNotWaitForAcknowledgementsAndConfirmsItsSends() {
        ActiveMQConnectionFactory factory = factory(LinkSettings.receiver(Guarantee.AT_LEAST_ONCE, 0));

        assertThat(factory.isPreAcknowledge()).isFalse();
        assertThat(factory.isBlockOnAcknowledge()).isFalse();
        assertThat(factory.isBlockOnDurableSend()).isTrue();
        assertThat(factory.getConsumerWindowSize()).isZero();
    }

    @Test
    void linksWithDifferentSettingsNeverShareAFactory() {
        assertThat(LinkSettings.receiver(Guarantee.AT_LEAST_ONCE, 0))
                .isNotEqualTo(LinkSettings.receiver(Guarantee.AT_LEAST_ONCE, 1024))
                .isNotEqualTo(LinkSettings.receiver(Guarantee.AT_MOST_ONCE, 0))
                .isNotEqualTo(LinkSettings.sender(Guarantee.AT_LEAST_ONCE));
        assertThat(LinkSettings.sender(Guarantee.AT_LEAST_ONCE).toString())
                .isNotEqualTo(LinkSettings.sender(Guarantee.AT_MOST_ONCE).toString());
    }
}
