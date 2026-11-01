package io.github.sudoitir.openbroker.rabbitmq.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.AMQImpl;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Backoff;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.FlowControl;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import io.github.sudoitir.openbroker.messaging.Observation;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.Recovery;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqKinds;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class RabbitMqReceiverTest {

    private static final ResourceId QUEUE = ResourceId.of(RabbitMqKinds.QUORUM_QUEUE.value(), "/", "orders");
    private static final Duration SHORT = Duration.ofMillis(50);

    private final MessagingFixtures fixtures = new MessagingFixtures();
    private RabbitMqReceiver receiver;

    @AfterEach
    void close() {
        if (receiver != null) {
            receiver.close();
        }
    }

    private RabbitMqReceiver open(ReceiverOptions options) {
        receiver = new RabbitMqReceiver(fixtures.env, QUEUE, options);
        return receiver;
    }

    private Channel channel(int index) {
        return fixtures.channels.get(index);
    }

    private DeliverCallback consumer(Channel channel, boolean autoAck) throws IOException {
        ArgumentCaptor<DeliverCallback> deliver = ArgumentCaptor.forClass(DeliverCallback.class);
        verify(channel)
                .basicConsume(
                        eq("orders"), eq(autoAck), any(String.class), deliver.capture(), any(CancelCallback.class));
        return deliver.getValue();
    }

    private static void deliver(DeliverCallback consumer, long tag) throws IOException {
        AMQP.BasicProperties props =
                new AMQP.BasicProperties.Builder().messageId("m" + tag).build();
        consumer.handle("tag", new Delivery(new Envelope(tag, false, "", "orders"), props, new byte[] {1, 2}));
    }

    private Message received(long tag) throws Exception {
        DeliverCallback consumer = consumer(channel(0), false);
        deliver(consumer, tag);
        List<Message> out = receiver.receive(5, Duration.ofSeconds(5));
        assertThat(out).hasSize(1);
        return out.getFirst();
    }

    private static void stale(Runnable call) {
        assertThatThrownBy(call::run).isInstanceOfSatisfying(BrokerException.class, e -> {
            assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
            assertThat(e.reason()).isEqualTo(ErrorReasons.DELIVERY_STALE);
        });
    }

    @Test
    void itConsumesWithManualAcksAndTheFlowControlAsQos() throws Exception {
        open(ReceiverOptions.consume().withFlow(new FlowControl(7, 0, 0)));
        verify(channel(0)).basicQos(7);
        consumer(channel(0), false);
        assertThat(receiver.state()).isEqualTo(LinkState.OPEN);
        assertThat(receiver.id()).isNotBlank();
    }

    @Test
    void aMessageCarriesTheGenerationAndTheTag() throws Exception {
        open(ReceiverOptions.consume());
        Message message = received(5);
        assertThat(message.ref().position()).isEqualTo("0:5");
        assertThat(message.headers().id()).contains("m5");
        assertThat(receiver.receive(1, SHORT)).isEmpty();
    }

    @Test
    void acceptAcknowledges() throws Exception {
        open(ReceiverOptions.consume());
        Message message = received(5);
        receiver.settle(message, Disposition.accept());
        verify(channel(1 - 1)).basicAck(5, false);
        stale(() -> receiver.settle(message, Disposition.accept()));
    }

    @Test
    void releaseRequeuesAndRejectDoesNot() throws Exception {
        open(ReceiverOptions.consume());
        DeliverCallback consumer = consumer(channel(0), false);
        deliver(consumer, 1);
        deliver(consumer, 2);
        List<Message> both = receiver.receive(5, Duration.ofSeconds(5));
        assertThat(both).hasSize(2);
        receiver.settle(both.get(0), new Disposition.Release(Optional.empty(), false));
        receiver.settle(both.get(1), new Disposition.Reject(Optional.of("bad")));
        verify(channel(0)).basicNack(1, false, true);
        verify(channel(0)).basicReject(2, false);
    }

    @Test
    void aDelayedReleaseIsHeldAndThenRequeued() throws Exception {
        open(ReceiverOptions.consume());
        Message message = received(3);
        receiver.settle(message, new Disposition.Release(Optional.of(Duration.ofSeconds(30)), true));
        verify(channel(0), timeout(5000)).basicNack(3, false, true);
        stale(() -> receiver.settle(message, Disposition.accept()));
    }

    @Test
    void aChannelThatIsGoneRefusesTheSettlement() throws Exception {
        open(ReceiverOptions.consume());
        Message message = received(5);
        when(channel(0).isOpen()).thenReturn(false);
        stale(() -> receiver.settle(message, Disposition.accept()));
        verify(channel(0), never()).basicAck(anyLong(), anyBoolean());
    }

    private static long anyLong() {
        return org.mockito.ArgumentMatchers.anyLong();
    }

    @Test
    void aSettlementAfterTheChannelWasReplacedIsStale() throws Exception {
        open(ReceiverOptions.consume());
        Message message = received(5);
        CountDownLatch reopened = MessagingFixtures.awaiting(fixtures.env, LinkState.OPEN);
        ArgumentCaptor<ShutdownListener> shutdown = ArgumentCaptor.forClass(ShutdownListener.class);
        verify(channel(0)).addShutdownListener(shutdown.capture());

        shutdown.getValue()
                .shutdownCompleted(new ShutdownSignalException(
                        false, false, new AMQImpl.Channel.Close(320, "forced", 0, 0), null));
        MessagingFixtures.await(reopened);

        stale(() -> receiver.settle(message, Disposition.accept()));
        verify(channel(1)).basicQos(100);
        DeliverCallback again = consumer(channel(1), false);
        deliver(again, 5);
        Message redelivered = receiver.receive(1, Duration.ofSeconds(5)).getFirst();
        assertThat(redelivered.ref().position()).isEqualTo("1:5");
        receiver.settle(redelivered, Disposition.accept());
        verify(channel(1)).basicAck(5, false);
    }

    @Test
    void aCancelFromTheBrokerIsALinkLoss() throws Exception {
        open(ReceiverOptions.consume());
        CountDownLatch reopened = MessagingFixtures.awaiting(fixtures.env, LinkState.OPEN);
        ArgumentCaptor<CancelCallback> cancel = ArgumentCaptor.forClass(CancelCallback.class);
        verify(channel(0))
                .basicConsume(any(), anyBoolean(), any(String.class), any(DeliverCallback.class), cancel.capture());

        cancel.getValue().handle("tag");

        MessagingFixtures.await(reopened);
        assertThat(fixtures.channels).hasSize(2);
    }

    @Test
    void aLinkThatCannotBeReopenedFails() throws Exception {
        open(ReceiverOptions.consume()
                .withRecovery(new Recovery(Backoff.defaults(), Optional.of(Duration.ofMillis(1)))));
        CountDownLatch failed = MessagingFixtures.awaiting(fixtures.env, LinkState.FAILED);
        ArgumentCaptor<ShutdownListener> shutdown = ArgumentCaptor.forClass(ShutdownListener.class);
        verify(channel(0)).addShutdownListener(shutdown.capture());
        when(fixtures.connection.createChannel()).thenThrow(new IOException("down"));

        shutdown.getValue()
                .shutdownCompleted(new ShutdownSignalException(
                        false, false, new AMQImpl.Connection.Close(320, "forced", 0, 0), null));
        MessagingFixtures.await(failed);

        assertThatThrownBy(() -> receiver.receive(1, SHORT)).isInstanceOf(BrokerException.class);
        assertThat(receiver.state()).isEqualTo(LinkState.FAILED);
    }

    @Test
    void atMostOnceAcknowledgesOnDeliveryAndHasNothingToSettle() throws Exception {
        open(ReceiverOptions.consume().withGuarantee(Guarantee.AT_MOST_ONCE));
        DeliverCallback consumer = consumer(channel(0), true);
        deliver(consumer, 1);
        Message message = receiver.receive(1, Duration.ofSeconds(5)).getFirst();

        receiver.settle(message, Disposition.accept());
        assertThatThrownBy(() -> receiver.settle(message, new Disposition.Reject(Optional.empty())))
                .isInstanceOf(BrokerException.class);
        verify(channel(0), never()).basicAck(anyLong(), anyBoolean());
    }

    @Test
    void anUnreadableDeliveryIsRejected() throws Exception {
        open(ReceiverOptions.consume());
        DeliverCallback consumer = consumer(channel(0), false);
        AMQP.BasicProperties bad =
                new AMQP.BasicProperties.Builder().priority(1).build();
        Delivery delivery = mock(Delivery.class);
        when(delivery.getEnvelope()).thenReturn(new Envelope(9, false, "", "orders"));
        when(delivery.getProperties()).thenReturn(bad);
        when(delivery.getBody()).thenThrow(new IllegalStateException("unreadable"));
        consumer.handle("tag", delivery);
        verify(channel(0)).basicReject(9, false);
    }

    @Test
    void observersSeeReceivesAndSettlements() throws Exception {
        MessagingObserver observer = mock(MessagingObserver.class);
        Observation observation = mock(Observation.class);
        when(observer.onReceive(any())).thenReturn(observation);
        when(observer.onSettle(any(), any(), any())).thenReturn(observation);
        MessagingFixtures observed =
                new MessagingFixtures(ConnectOptions.defaults().withObserver(observer));
        receiver = new RabbitMqReceiver(observed.env, QUEUE, ReceiverOptions.consume());
        Channel channel = observed.channels.get(0);
        ArgumentCaptor<DeliverCallback> deliver = ArgumentCaptor.forClass(DeliverCallback.class);
        verify(channel)
                .basicConsume(any(), anyBoolean(), any(String.class), deliver.capture(), any(CancelCallback.class));
        deliver(deliver.getValue(), 1);

        Message message = receiver.receive(1, Duration.ofSeconds(5)).getFirst();
        receiver.settle(message, Disposition.accept());

        verify(observation, org.mockito.Mockito.times(2)).end(null);
    }

    @Test
    void whatTheBrokerCannotDoIsRefusedBeforeAnyChannel() {
        ResourceId stream = ResourceId.of(RabbitMqKinds.STREAM.value(), "/", "s");
        assertThatThrownBy(() -> RabbitMqReceiver.check(stream, ReceiverOptions.consume()))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED));
        assertThatThrownBy(() -> RabbitMqReceiver.check(QUEUE, ReceiverOptions.browse(Optional.empty())))
                .isInstanceOf(BrokerException.class);
        assertThatThrownBy(() -> RabbitMqReceiver.check(
                        QUEUE, ReceiverOptions.consume().producedAtOrBefore(java.time.Instant.now())))
                .isInstanceOf(BrokerException.class);
        assertThatThrownBy(() -> RabbitMqReceiver.check(
                        QUEUE,
                        new ReceiverOptions(
                                io.github.sudoitir.openbroker.messaging.ReceiveMode.CONSUME,
                                Guarantee.AT_LEAST_ONCE,
                                Optional.of(mock(io.github.sudoitir.openbroker.messaging.MessageFilter.class)),
                                Optional.empty(),
                                FlowControl.defaults(),
                                Recovery.defaults())))
                .isInstanceOf(BrokerException.class);
        assertThatThrownBy(
                        () -> RabbitMqReceiver.check(ResourceId.of("rabbitmq.queue", "q"), ReceiverOptions.consume()))
                .isInstanceOf(BrokerException.class);
        assertThat(fixtures.channels).isEmpty();
    }

    @Test
    void transactionsAreRefusedAndCloseIsFinal() throws Exception {
        open(ReceiverOptions.consume());
        assertThatThrownBy(receiver::commit).isInstanceOf(BrokerException.class);
        assertThatThrownBy(receiver::rollback).isInstanceOf(BrokerException.class);
        SendTarget reply = SendTarget.subscription(QUEUE);
        assertThatThrownBy(() -> receiver.sender(reply)).isInstanceOf(BrokerException.class);
        receiver.close();
        receiver.close();
        assertThat(receiver.state()).isEqualTo(LinkState.CLOSED);
        assertThatThrownBy(() -> receiver.receive(1, SHORT)).isInstanceOf(BrokerException.class);
        verify(channel(0)).close();
        assertThat(new AtomicInteger()).hasValue(0);
    }
}
