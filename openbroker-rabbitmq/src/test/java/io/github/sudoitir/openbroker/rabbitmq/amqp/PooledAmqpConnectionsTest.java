package io.github.sudoitir.openbroker.rabbitmq.amqp;

import static io.github.sudoitir.openbroker.rabbitmq.amqp.AmqpTestClusters.cluster;
import static io.github.sudoitir.openbroker.rabbitmq.amqp.AmqpTestClusters.data;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.AuthenticationFailureException;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.AMQImpl;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class PooledAmqpConnectionsTest {

    private final Deadline later = Deadline.after(Duration.ofSeconds(30), Clock.systemUTC());
    private final ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
    private final AmqpFactory amqpFactory = mock(AmqpFactory.class);
    private RabbitMqCluster cluster;
    private PooledAmqpConnections connections;

    @BeforeEach
    void setUp() {
        cluster = cluster(Credentials.usernamePassword("u", "p"), ConnectOptions.defaults(), data("amqp", "h"));
        when(amqpFactory.factory(anyString())).thenReturn(connectionFactory);
        when(amqpFactory.addresses()).thenReturn(List.of(new Address("h", 5672)));
        when(amqpFactory.name(anyString())).thenReturn("openbroker-test");
        connections = new PooledAmqpConnections(cluster, amqpFactory);
    }

    private Connection open() throws Exception {
        Connection connection = mock(Connection.class);
        when(connection.isOpen()).thenReturn(true);
        when(connectionFactory.newConnection(anyList(), anyString())).thenReturn(connection);
        return connection;
    }

    private static ShutdownSignalException shutdown(boolean byApplication) {
        return new ShutdownSignalException(
                true, byApplication, new AMQImpl.Connection.Close(320, "forced", 0, 0), null);
    }

    private static ShutdownListener listenerOf(Connection connection) {
        ArgumentCaptor<ShutdownListener> captor = ArgumentCaptor.forClass(ShutdownListener.class);
        verify(connection).addShutdownListener(captor.capture());
        return captor.getValue();
    }

    @Test
    void aConnectionIsOpenedOnFirstUseAndReusedPerVirtualHost() throws Exception {
        Connection first = open();

        assertThat(connections.connection("/", later)).isSameAs(first);
        assertThat(connections.connection("/", later)).isSameAs(first);
        verify(connectionFactory, times(1)).newConnection(anyList(), anyString());

        Connection second = open();
        assertThat(connections.connection("orders", later)).isSameAs(second);
        verify(connectionFactory, times(2)).newConnection(anyList(), anyString());
        verify(connectionFactory, times(2)).setConnectionTimeout(anyInt());
        verify(connectionFactory, times(2)).setHandshakeTimeout(anyInt());
    }

    @Test
    void nothingIsOpenedBeforeItIsAsked() throws Exception {
        verify(connectionFactory, never()).newConnection(anyList(), anyString());
        assertThat(connections).isNotNull();
    }

    @Test
    void aClosedConnectionIsReopened() throws Exception {
        Connection first = open();
        connections.connection("/", later);
        when(first.isOpen()).thenReturn(false);
        Connection second = open();

        assertThat(connections.connection("/", later)).isSameAs(second);
    }

    @Test
    void aShutdownTheApplicationDidNotStartIsALoss() throws Exception {
        Connection connection = open();
        AtomicInteger lost = new AtomicInteger();
        AtomicInteger other = new AtomicInteger();
        connections.onLoss("/", lost::incrementAndGet);
        connections.onLoss("orders", other::incrementAndGet);
        connections.connection("/", later);

        listenerOf(connection).shutdownCompleted(shutdown(false));

        assertThat(lost).hasValue(1);
        assertThat(other).hasValue(0);
        Connection again = open();
        assertThat(connections.connection("/", later)).isSameAs(again);
    }

    @Test
    void aShutdownTheApplicationStartedIsNotALoss() throws Exception {
        Connection connection = open();
        AtomicInteger lost = new AtomicInteger();
        connections.onLoss("/", lost::incrementAndGet);
        connections.connection("/", later);

        listenerOf(connection).shutdownCompleted(shutdown(true));

        assertThat(lost).hasValue(0);
    }

    @Test
    void aClosedHandleStopsTheListener() throws Exception {
        Connection connection = open();
        AtomicInteger lost = new AtomicInteger();
        AutoCloseable handle = connections.onLoss("/", lost::incrementAndGet);
        connections.connection("/", later);
        handle.close();

        listenerOf(connection).shutdownCompleted(shutdown(false));

        assertThat(lost).hasValue(0);
    }

    @Test
    void reconfiguringClosesEveryConnectionAndTheNextCallReopens() throws Exception {
        Connection first = open();
        connections.connection("/", later);
        AtomicInteger lost = new AtomicInteger();
        connections.onLoss("/", lost::incrementAndGet);

        cluster.reconfigure(new ClusterSpec(List.of(), List.of(data("amqp", "other")), Map.of()));

        verify(first).close(200, "closed by the application", 1000);
        Connection second = open();
        assertThat(connections.connection("/", later)).isSameAs(second);
        assertThat(lost).hasValue(0);
    }

    @Test
    void closingTheClusterClosesTheConnectionsAndRefusesMore() throws Exception {
        Connection first = open();
        connections.connection("/", later);

        cluster.close();

        verify(first).close(200, "closed by the application", 1000);
        assertThatThrownBy(() -> connections.connection("/", later))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT));
    }

    @Test
    void aFailingCloseIsIgnored() throws Exception {
        Connection first = open();
        connections.connection("/", later);
        org.mockito.Mockito.doThrow(new IOException("gone")).when(first).close(anyInt(), anyString(), anyInt());

        connections.close();

        verify(first).close(200, "closed by the application", 1000);
    }

    @Test
    void anExpiredDeadlineOpensNothing() throws Exception {
        Deadline past = Deadline.after(Duration.ofSeconds(-1), Clock.systemUTC());

        assertThatThrownBy(() -> connections.connection("/", past)).isInstanceOfSatisfying(BrokerException.class, e -> {
            assertThat(e.status()).isEqualTo(Status.DEADLINE_EXCEEDED);
            assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
        });
        verify(connectionFactory, never()).newConnection(anyList(), anyString());
    }

    @Test
    void failuresToConnectAreMapped() throws Exception {
        when(connectionFactory.newConnection(anyList(), anyString()))
                .thenThrow(new AuthenticationFailureException("bad"))
                .thenThrow(new TimeoutException("slow"));

        assertThatThrownBy(() -> connections.connection("/", later))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.UNAUTHENTICATED));
        assertThatThrownBy(() -> connections.connection("/", later))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.DEADLINE_EXCEEDED));
    }

    @Test
    void theClusterConstructorBuildsItsOwnFactory() {
        assertThat(new PooledAmqpConnections(cluster)).isNotNull();
    }
}
