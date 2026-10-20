package io.github.sudoitir.openbroker.artemis.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.Timeouts;
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

    @Test
    void aConnectionNeverRetriesOnItsOwnAndTimesOutWithTheCaller() {
        ActiveMQConnectionFactory factory =
                CoreLinks.connectionFactory(OPTIONS, "tcp://broker:61616", Optional.empty());

        assertThat(factory.getInitialConnectAttempts()).isEqualTo(1);
        assertThat(factory.getReconnectAttempts()).isZero();
        assertThat(factory.getCallTimeout()).isEqualTo(7_000);
    }

    @Test
    void theBrokerDropsASilentConnectionOnlyAfterSeveralMissedPings() {
        ActiveMQConnectionFactory factory =
                CoreLinks.connectionFactory(OPTIONS, "tcp://broker:61616", Optional.empty());

        assertThat(factory.getClientFailureCheckPeriod()).isEqualTo(7_000);
        assertThat(factory.getConnectionTTL()).isEqualTo(21_000);
    }

    @Test
    void aConsumerHoldsAtMostOneWindowAndTheTopologyNeverRedirectsIt() {
        ActiveMQConnectionFactory factory = CoreLinks.connectionFactory(OPTIONS, "broker:61616", Optional.empty());

        assertThat(factory.getConsumerWindowSize()).isEqualTo(CoreLinks.WINDOW_BYTES);
        assertThat(factory.getServerLocator().getUseTopologyForLoadBalancing()).isFalse();
        assertThat(factory.getServerLocator()
                        .getStaticTransportConfigurations()[0]
                        .getParams())
                .containsEntry("host", "broker")
                .containsEntry(TransportConstants.NETTY_CONNECT_TIMEOUT, "2000");
    }
}
