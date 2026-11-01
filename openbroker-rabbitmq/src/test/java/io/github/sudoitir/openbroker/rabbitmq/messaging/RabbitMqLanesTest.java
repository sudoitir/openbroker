package io.github.sudoitir.openbroker.rabbitmq.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.Processor;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.RetryPolicy;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqKinds;
import io.github.sudoitir.openbroker.support.messaging.ProcessorEngine;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class RabbitMqLanesTest {

    private static final ResourceId QUEUE = ResourceId.of(RabbitMqKinds.QUORUM_QUEUE.value(), "/", "orders");
    private static final ResourceId DEAD = ResourceId.of(RabbitMqKinds.CLASSIC_QUEUE.value(), "/", "dead");

    private final MessagingFixtures fixtures = new MessagingFixtures();

    private static void deliver(Channel channel, long tag) throws Exception {
        ArgumentCaptor<DeliverCallback> deliver = ArgumentCaptor.forClass(DeliverCallback.class);
        verify(channel, timeout(5000))
                .basicConsume(any(), anyBoolean(), any(String.class), deliver.capture(), any(CancelCallback.class));
        deliver.getValue()
                .handle(
                        "t",
                        new Delivery(
                                new Envelope(tag, false, "", "orders"),
                                new AMQP.BasicProperties.Builder()
                                        .messageId("m" + tag)
                                        .build(),
                                new byte[] {1}));
    }

    @Test
    void aLaneHoldsOneMessageAndSettlesIt() throws Exception {
        DeadLetterSink.Senders dead = new DeadLetterSink.Senders(fixtures.env, RetryPolicy.defaults());
        RabbitMqLanes lanes = new RabbitMqLanes(fixtures.env, "p", QUEUE, ProcessorOptions.defaults(), dead);
        assertThat(lanes.state()).isEqualTo(LinkState.OPEN);
        assertThat(lanes.pinsGroups()).isFalse();
        assertThat(lanes.processorId()).isEqualTo("p");
        ProcessorEngine.Lane lane = lanes.open(0);
        verify(fixtures.channels.get(0)).basicQos(1);
        deliver(fixtures.channels.get(0), 4);

        Message message = lane.next(Duration.ofSeconds(5)).orElseThrow();
        assertThat(lane.next(Duration.ofMillis(20))).isEmpty();
        lane.settle(message, new Disposition.Reject(Optional.of("no")));
        verify(fixtures.channels.get(0)).basicReject(4, false);
        assertThatThrownBy(() -> lane.sender(SendTarget.subscription(QUEUE))).isInstanceOf(BrokerException.class);
        lane.close();
        assertThat(lanes.state()).isEqualTo(LinkState.CLOSED);
        lanes.close();
        dead.close();
    }

    @Test
    void aRejectGoesToThePolicysTargetBeforeItIsAcknowledged() throws Exception {
        RetryPolicy policy = RetryPolicy.managed(3, io.github.sudoitir.openbroker.messaging.Backoff.defaults())
                .withDeadLetter(SendTarget.subscription(DEAD));
        ProcessorOptions options = ProcessorOptions.defaults().withRetry(policy);
        DeadLetterSink.Senders dead = new DeadLetterSink.Senders(fixtures.env, policy);
        RabbitMqLanes lanes = new RabbitMqLanes(fixtures.env, "p", QUEUE, options, dead);
        ProcessorEngine.Lane lane = lanes.open(0);
        Channel consuming = fixtures.channels.get(0);
        deliver(consuming, 4);
        Message message = lane.next(Duration.ofSeconds(5)).orElseThrow();

        // The dead-letter sender opens a passive channel and its own channel (the mocks hand out two more).
        Thread settling =
                Thread.ofVirtual().start(() -> lane.settle(message, new Disposition.Reject(Optional.of("no"))));
        verify(fixtures.connection, timeout(10_000).atLeast(3)).createChannel();
        Channel publishing = fixtures.channel(2);
        verify(publishing, timeout(10_000))
                .basicPublish(eq(""), eq("dead"), eq(true), any(AMQP.BasicProperties.class), any());
        com.rabbitmq.client.ConfirmCallback ack = confirm(publishing);
        ack.handle(1, false);
        verify(consuming, timeout(10_000)).basicAck(4, false);
        settling.join(TimeUnit.SECONDS.toMillis(10));
        lanes.close();
        dead.close();
    }

    private static com.rabbitmq.client.ConfirmCallback confirm(Channel channel) {
        ArgumentCaptor<com.rabbitmq.client.ConfirmCallback> ack =
                ArgumentCaptor.forClass(com.rabbitmq.client.ConfirmCallback.class);
        verify(channel).addConfirmListener(ack.capture(), any(com.rabbitmq.client.ConfirmCallback.class));
        return ack.getValue();
    }

    @Test
    void aTransportRunsAProcessorOverTheLanes() throws Exception {
        RabbitMqTransport transport = new RabbitMqTransport(fixtures.cluster, fixtures.connections);
        CountDownLatch handled = new CountDownLatch(1);
        Processor processor = transport.process(
                QUEUE, Scope.whole(), ProcessorOptions.defaults().withConcurrency(1), delivery -> {
                    handled.countDown();
                    return Disposition.accept();
                });
        assertThat(processor.state()).isEqualTo(LinkState.OPEN);
        assertThat(processor.retry().mode()).isEqualTo(io.github.sudoitir.openbroker.messaging.RetryMode.SYSTEM);
        Channel channel = fixtures.channels.get(0);
        deliver(channel, 7);
        assertThat(handled.await(10, TimeUnit.SECONDS)).isTrue();
        verify(channel, timeout(10_000)).basicAck(7, false);

        processor.stop(Duration.ofSeconds(5));
        processor.close();
        assertThat(processor.id()).isNotBlank();
        transport.close();
    }

    @Test
    void aTransportOpensReceiversAndSenders() throws Exception {
        RabbitMqTransport transport = new RabbitMqTransport(fixtures.cluster, fixtures.connections);
        var receiver = transport.receiver(
                QUEUE, Scope.whole(), io.github.sudoitir.openbroker.messaging.ReceiverOptions.consume());
        var sender = transport.sender(SendTarget.subscription(DEAD), Scope.whole(), SenderOptions.defaults());
        assertThat(receiver.state()).isEqualTo(LinkState.OPEN);
        assertThat(sender.state()).isEqualTo(LinkState.OPEN);
        transport.close();
        assertThat(receiver.state()).isEqualTo(LinkState.CLOSED);
        assertThat(sender.state()).isEqualTo(LinkState.CLOSED);
    }

    @Test
    void anOwnedTransportClosesItsConnections() {
        RabbitMqTransport transport = new RabbitMqTransport(fixtures.cluster);
        assertThat(transport.capabilities()).isNotNull();
        transport.close();
    }
}
