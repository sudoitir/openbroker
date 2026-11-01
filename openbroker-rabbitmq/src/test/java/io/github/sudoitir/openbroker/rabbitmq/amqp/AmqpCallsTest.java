package io.github.sudoitir.openbroker.rabbitmq.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.AMQImpl;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AmqpCallsTest {

    private final Deadline later = Deadline.after(Duration.ofSeconds(30), Clock.systemUTC());
    private final AmqpConnections connections = mock(AmqpConnections.class);
    private final Connection connection = mock(Connection.class);
    private final Channel channel = mock(Channel.class);
    private final AmqpCalls calls = new AmqpCalls(connections);

    @BeforeEach
    void setUp() throws IOException {
        when(connections.connection(any(), any())).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);
        when(channel.isOpen()).thenReturn(true);
    }

    private static IOException closedWith(int code, String text) {
        return new IOException(
                "channel closed",
                new ShutdownSignalException(false, false, new AMQImpl.Channel.Close(code, text, 0, 0), null));
    }

    @Test
    void aPassiveQueueDeclareReturnsTheCounts() throws Exception {
        AMQP.Queue.DeclareOk ok = new AMQImpl.Queue.DeclareOk("orders", 3, 1);
        when(channel.queueDeclarePassive("orders")).thenReturn(ok);

        assertThat(calls.queueDeclarePassive("/", "orders", later)).isSameAs(ok);

        verify(channel).close();
    }

    @Test
    void aMissingQueueIsNotFound() throws IOException {
        when(channel.queueDeclarePassive("orders")).thenThrow(closedWith(404, "NOT_FOUND - no queue 'orders'"));

        assertThatThrownBy(() -> calls.queueDeclarePassive("/", "orders", later))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.status()).isEqualTo(Status.NOT_FOUND);
                    assertThat(e.reason()).isEqualTo(ErrorReasons.RESOURCE_GONE);
                    assertThat(e.dispatch()).isEqualTo(Dispatch.REJECTED);
                });
    }

    @Test
    void aMissingExchangeNamesTheExchange() throws IOException {
        when(channel.exchangeDeclarePassive("ex")).thenThrow(closedWith(404, "NOT_FOUND - no exchange 'ex'"));

        assertThatThrownBy(() -> calls.exchangeDeclarePassive("/", "ex", later))
                .isInstanceOfSatisfying(
                        BrokerException.class,
                        e -> assertThat(e.info().metadata())
                                .containsEntry(ErrorReasons.RESOURCE, "rabbitmq.exchange/%2F/ex"));
    }

    @Test
    void anExchangeThatExistsReturnsNormally() throws Exception {
        when(channel.exchangeDeclarePassive("ex")).thenReturn(new AMQImpl.Exchange.DeclareOk());

        calls.exchangeDeclarePassive("/", "ex", later);

        verify(channel).close();
    }

    @Test
    void anIoFailureWithoutAReplyIsMapped() throws IOException {
        when(channel.queueDeclarePassive("orders")).thenThrow(new IOException("reset"));

        assertThatThrownBy(() -> calls.queueDeclarePassive("/", "orders", later))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.UNAVAILABLE));
    }

    @Test
    void aChannelThatCannotBeOpenedIsMapped() throws IOException {
        when(connection.createChannel()).thenThrow(closedWith(530, "NOT_ALLOWED - access to vhost 'x' refused"));

        assertThatThrownBy(() -> calls.queueDeclarePassive("x", "q", later))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.PERMISSION_DENIED));
    }

    @Test
    void aChannelThatFailsToCloseIsIgnored() throws Exception {
        when(channel.queueDeclarePassive("orders")).thenReturn(new AMQImpl.Queue.DeclareOk("orders", 0, 0));
        org.mockito.Mockito.doThrow(new IOException("gone")).when(channel).close();

        assertThat(calls.queueDeclarePassive("/", "orders", later).getQueue()).isEqualTo("orders");
    }

    @Test
    void anExpiredDeadlineSendsNothing() {
        Deadline past = Deadline.after(Duration.ofSeconds(-1), Clock.systemUTC());

        assertThatThrownBy(() -> calls.queueDeclarePassive("/", "orders", past))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.status()).isEqualTo(Status.DEADLINE_EXCEEDED);
                    assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
                });
    }
}
