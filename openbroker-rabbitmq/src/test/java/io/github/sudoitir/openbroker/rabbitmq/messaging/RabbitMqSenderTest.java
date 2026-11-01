package io.github.sudoitir.openbroker.rabbitmq.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Return;
import com.rabbitmq.client.ReturnCallback;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.AMQImpl;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.Offer;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.model.PartitionId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqKinds;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class RabbitMqSenderTest {

    private static final ResourceId EXCHANGE = ResourceId.of(RabbitMqKinds.EXCHANGE.value(), "/", "ex");
    private static final ResourceId QUEUE = ResourceId.of(RabbitMqKinds.CLASSIC_QUEUE.value(), "/", "orders");

    private final MessagingFixtures fixtures = new MessagingFixtures();
    private RabbitMqSender sender;

    @AfterEach
    void close() {
        if (sender != null) {
            sender.close();
        }
    }

    private static OutboundMessage message(String id) {
        return new OutboundMessage(
                MessageHeaders.builder().id(id).build(),
                java.util.Map.of(),
                MessageBody.of(new byte[] {1}),
                java.util.Map.of());
    }

    private RabbitMqSender open(SendTarget target, SenderOptions options) {
        sender = new RabbitMqSender(fixtures.env, target, options);
        return sender;
    }

    /** The channel of the link: the first is the passive declare of the target. */
    private Channel link(int generation) {
        return fixtures.channel(generation * 2 + 1);
    }

    private ConfirmCallback ack(Channel channel) throws IOException {
        ArgumentCaptor<ConfirmCallback> ack = ArgumentCaptor.forClass(ConfirmCallback.class);
        verify(channel).addConfirmListener(ack.capture(), any(ConfirmCallback.class));
        return ack.getValue();
    }

    private ConfirmCallback nack(Channel channel) throws IOException {
        ArgumentCaptor<ConfirmCallback> nack = ArgumentCaptor.forClass(ConfirmCallback.class);
        verify(channel).addConfirmListener(any(ConfirmCallback.class), nack.capture());
        return nack.getValue();
    }

    private static BrokerException failureOf(CompletionStage<SendReceipt> receipt) throws InterruptedException {
        try {
            receipt.toCompletableFuture().get(10, TimeUnit.SECONDS);
            throw new AssertionError("expected a failure");
        } catch (ExecutionException e) {
            return (BrokerException) e.getCause();
        } catch (java.util.concurrent.TimeoutException e) {
            throw new AssertionError(e);
        }
    }

    private CompletionStage<SendReceipt> offered(OutboundMessage message) {
        Offer offer = sender.offer(message);
        return ((Offer.Accepted) offer).receipt();
    }

    @Test
    void aConfirmedSendPublishesMandatoryAndResolvesOnTheAck() throws Exception {
        open(new SendTarget.ToDestination(EXCHANGE, Optional.of("rk"), Optional.empty()), SenderOptions.defaults());
        Channel channel = link(0);
        verify(channel).confirmSelect();
        verify(fixtures.channels.get(0)).exchangeDeclarePassive("ex");

        CompletionStage<SendReceipt> receipt = offered(message("m1"));
        verify(channel, timeout(5000))
                .basicPublish(eq("ex"), eq("rk"), eq(true), any(AMQP.BasicProperties.class), any());
        ack(channel).handle(1, false);

        SendReceipt done = receipt.toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(done.messageId()).contains("m1");
        assertThat(done.resent()).isFalse();
        assertThat(sender.state()).isEqualTo(LinkState.OPEN);
    }

    @Test
    void aReturnBeforeTheAckFailsNotRouted() throws Exception {
        open(SendTarget.subscription(QUEUE), SenderOptions.defaults());
        Channel channel = link(0);
        verify(fixtures.channels.get(0)).queueDeclarePassive("orders");
        ArgumentCaptor<ReturnCallback> returned = ArgumentCaptor.forClass(ReturnCallback.class);
        verify(channel).addReturnListener(returned.capture());

        CompletionStage<SendReceipt> receipt = offered(message("m1"));
        ArgumentCaptor<AMQP.BasicProperties> sent = ArgumentCaptor.forClass(AMQP.BasicProperties.class);
        verify(channel, timeout(5000)).basicPublish(eq(""), eq("orders"), eq(true), sent.capture(), any());
        returned.getValue().handle(new Return(312, "NO_ROUTE", "", "orders", sent.getValue(), new byte[] {1}));
        ack(channel).handle(1, false);

        BrokerException e = failureOf(receipt);
        assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
        assertThat(e.reason()).isEqualTo(ErrorReasons.NOT_ROUTED);
    }

    @Test
    void aNackFailsDestinationFull() throws Exception {
        open(SendTarget.subscription(QUEUE), SenderOptions.defaults());
        Channel channel = link(0);
        CompletionStage<SendReceipt> receipt = offered(message("m1"));
        verify(channel, timeout(5000)).basicPublish(any(), any(), anyBoolean(), any(), any());
        nack(channel).handle(1, false);

        assertThat(failureOf(receipt).reason()).isEqualTo(ErrorReasons.DESTINATION_FULL);
    }

    @Test
    void atMostOnceDoesNotUseConfirms() throws Exception {
        open(SendTarget.subscription(QUEUE), SenderOptions.defaults().withGuarantee(Guarantee.AT_MOST_ONCE));
        Channel channel = link(0);

        assertThat(sender.send(message("m1")).messageId()).contains("m1");

        verify(channel, never()).confirmSelect();
        verify(channel).basicPublish(eq(""), eq("orders"), eq(false), any(AMQP.BasicProperties.class), any());
    }

    @Test
    void aNonDurableMessageIsRefusedUnderAtLeastOnce() {
        open(SendTarget.subscription(QUEUE), SenderOptions.defaults());
        OutboundMessage transientMessage = message("m1")
                .withHeaders(MessageHeaders.builder().durable(false).build());

        assertThatThrownBy(() -> sender.send(transientMessage))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT));
        assertThatThrownBy(() -> sender.offer(transientMessage)).isInstanceOf(BrokerException.class);
    }

    @Test
    void aLostChannelIsRecoveredAndTheUnconfirmedMessageIsWrittenAgain() throws Exception {
        open(SendTarget.subscription(QUEUE), SenderOptions.defaults());
        Channel first = link(0);
        ArgumentCaptor<ShutdownListener> shutdown = ArgumentCaptor.forClass(ShutdownListener.class);
        verify(first).addShutdownListener(shutdown.capture());

        CompletionStage<SendReceipt> receipt = offered(message("m1"));
        verify(first, timeout(5000)).basicPublish(any(), any(), anyBoolean(), any(), any());
        var reopened = MessagingFixtures.awaiting(fixtures.env, LinkState.OPEN);
        shutdown.getValue()
                .shutdownCompleted(new ShutdownSignalException(
                        false, false, new AMQImpl.Channel.Close(320, "forced", 0, 0), null));

        verify(fixtures.connection, timeout(10_000).times(4)).createChannel();
        Channel second = link(1);
        verify(second, timeout(5000)).basicPublish(any(), any(), anyBoolean(), any(), any());
        ack(second).handle(1, false);

        assertThat(receipt.toCompletableFuture().get(10, TimeUnit.SECONDS).resent())
                .isTrue();
        assertThat(reopened).isNotNull();
        assertThat(sender.state()).isIn(LinkState.OPEN, LinkState.RECOVERING);
    }

    @Test
    void aPublishThatThrowsKeepsTheMessageForTheRecovery() throws Exception {
        open(SendTarget.subscription(QUEUE), SenderOptions.defaults());
        Channel first = link(0);
        org.mockito.Mockito.doThrow(new IOException("broken"))
                .when(first)
                .basicPublish(any(), any(), anyBoolean(), any(), any());

        CompletionStage<SendReceipt> receipt = offered(message("m1"));
        verify(fixtures.connection, timeout(10_000).times(4)).createChannel();
        Channel second = link(1);
        verify(second, timeout(5000)).basicPublish(any(), any(), anyBoolean(), any(), any());
        ack(second).handle(1, false);
        assertThat(receipt.toCompletableFuture().get(10, TimeUnit.SECONDS).messageId())
                .contains("m1");
    }

    @Test
    void aMissingTargetFailsTheOpenWithoutALinkChannel() throws Exception {
        org.mockito.Mockito.doAnswer(call -> {
                    Channel passive = org.mockito.Mockito.mock(Channel.class);
                    when(passive.isOpen()).thenReturn(true);
                    when(passive.queueDeclarePassive(any()))
                            .thenThrow(new IOException(
                                    "closed",
                                    new ShutdownSignalException(
                                            false,
                                            false,
                                            new AMQImpl.Channel.Close(404, "NOT_FOUND - no queue", 0, 0),
                                            null)));
                    fixtures.channels.add(passive);
                    return passive;
                })
                .when(fixtures.connection)
                .createChannel();

        assertThatThrownBy(() -> open(SendTarget.subscription(QUEUE), SenderOptions.defaults()))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.NOT_FOUND));
        assertThat(fixtures.channels).hasSize(1);
    }

    @Test
    void destinationsAreResolved() {
        RabbitMqSender.Destination named = RabbitMqSender.Destination.of(
                new SendTarget.ToDestination(EXCHANGE, Optional.empty(), Optional.empty()));
        assertThat(named.exchange()).isEqualTo("ex");
        assertThat(named.routingKey()).isEmpty();
        RabbitMqSender.Destination direct = RabbitMqSender.Destination.of(
                SendTarget.destination(ResourceId.of(RabbitMqKinds.EXCHANGE.value(), "/", "amq.default")));
        assertThat(direct.exchange()).isEmpty();
        RabbitMqSender.Destination queue = RabbitMqSender.Destination.of(SendTarget.destination(QUEUE));
        assertThat(queue.queue()).isTrue();
        assertThat(queue.routingKey()).isEqualTo("orders");
        assertThatThrownBy(() -> RabbitMqSender.Destination.of(
                        new SendTarget.ToDestination(EXCHANGE, Optional.empty(), Optional.of(PartitionId.of("0")))))
                .isInstanceOf(BrokerException.class);
        assertThatThrownBy(() -> RabbitMqSender.Destination.of(SendTarget.subscription(EXCHANGE)))
                .isInstanceOf(BrokerException.class);
        assertThatThrownBy(() -> RabbitMqSender.Destination.of(
                        SendTarget.destination(ResourceId.of(RabbitMqKinds.EXCHANGE.value(), "/"))))
                .isInstanceOf(BrokerException.class);
    }

    @Test
    void transactionsAreRefusedAndCloseIsFinal() throws Exception {
        open(SendTarget.subscription(QUEUE), SenderOptions.defaults());
        assertThatThrownBy(sender::commit).isInstanceOf(BrokerException.class);
        assertThatThrownBy(sender::rollback).isInstanceOf(BrokerException.class);
        assertThat(sender.deduplication()).isEmpty();
        assertThat(sender.id()).isNotBlank();
        sender.flush(Duration.ofSeconds(1));
        sender.close();
        sender.close();
        assertThat(sender.state()).isEqualTo(LinkState.CLOSED);
        verify(link(0)).close();
    }
}
