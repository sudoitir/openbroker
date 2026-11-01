package io.github.sudoitir.openbroker.rabbitmq.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.LongStringHelper;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.notification.BrokerEvent;
import io.github.sudoitir.openbroker.notification.EventTypes;
import io.github.sudoitir.openbroker.notification.Gap;
import io.github.sudoitir.openbroker.notification.NotificationListener;
import io.github.sudoitir.openbroker.notification.StreamState;
import io.github.sudoitir.openbroker.rabbitmq.amqp.AmqpConnections;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class EventExchangeStreamTest {

    private FakeManagement server;
    private AmqpConnections connections;
    private Connection connection;
    private Channel channel;
    private ScheduledExecutorService timer;
    private EventExchangeStream stream;
    private final BlockingQueue<Object> received = new LinkedBlockingQueue<>();

    @BeforeEach
    void start() throws Exception {
        server = new FakeManagement();
        server.json("GET", "/api/queues/%2F", "queues.json");
        connections = mock(AmqpConnections.class);
        connection = mock(Connection.class);
        channel = mock(Channel.class);
        timer = mock(ScheduledExecutorService.class);
        AMQP.Queue.DeclareOk declared = mock(AMQP.Queue.DeclareOk.class);
        when(declared.getQueue()).thenReturn("amq.gen-1");
        when(connections.connection(eq("/"), any(Deadline.class))).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);
        when(channel.queueDeclare("", false, true, true, null)).thenReturn(declared);
    }

    @AfterEach
    void stop() {
        if (stream != null) {
            stream.close();
        }
        server.close();
    }

    private EventExchangeStream stream() {
        RabbitMqCluster cluster = FakeManagement.cluster(
                Credentials.usernamePassword("ops", "s3cret"), ConnectOptions.defaults(), server.endpoint());
        NotificationListener listener = new NotificationListener() {
            @Override
            public void onEvent(BrokerEvent event) {
                received.add(event);
            }

            @Override
            public void onGap(Gap gap) {
                received.add(gap);
            }
        };
        stream = new EventExchangeStream(cluster, listener, connections, timer);
        return stream;
    }

    private DeliverCallback delivery() throws IOException {
        ArgumentCaptor<DeliverCallback> callback = ArgumentCaptor.forClass(DeliverCallback.class);
        verify(channel).basicConsume(eq("amq.gen-1"), eq(true), callback.capture(), any(CancelCallback.class));
        return callback.getValue();
    }

    private static Delivery message(String routingKey, Map<String, Object> headers) {
        return new Delivery(
                new Envelope(1, false, "amq.rabbitmq.event", routingKey),
                new AMQP.BasicProperties.Builder().headers(headers).build(),
                new byte[0]);
    }

    private Object next() throws InterruptedException {
        Object item = received.poll(5, TimeUnit.SECONDS);
        assertThat(item).isNotNull();
        return item;
    }

    @Test
    void bindsAnExclusiveServerNamedQueueToTheEventExchangeAndTellsTheListenerItStarted() throws Exception {
        EventExchangeStream s = stream();
        s.start();
        verify(channel).queueBind("amq.gen-1", "amq.rabbitmq.event", "#");
        assertThat(s.state().phase()).isEqualTo(StreamState.Phase.ACTIVE);
        Object first = next();
        assertThat(first).isInstanceOf(Gap.class);
        assertThat(((Gap) first).cause()).isEqualTo(Gap.Cause.STARTED);
    }

    @Test
    void mapsWhatItReadsAndReadsAQueuesKindFromTheListing() throws Exception {
        EventExchangeStream s = stream();
        s.start();
        next();
        DeliverCallback callback = delivery();
        callback.handle(
                "tag",
                message(
                        "consumer.created",
                        Map.of(
                                "queue", LongStringHelper.asLongString("payments"),
                                "vhost", LongStringHelper.asLongString("/"),
                                "consumer_tag", LongStringHelper.asLongString("ctag"),
                                "channel", LongStringHelper.asLongString("<rabbit@a.1.2>"),
                                "arguments", Map.of("x", LongStringHelper.asLongString("y")))));
        BrokerEvent attached = (BrokerEvent) next();
        assertThat(attached.type()).isEqualTo(EventTypes.CONSUMER_ATTACHED);
        assertThat(attached.subject().orElseThrow().kind()).isEqualTo(RabbitMqKinds.QUORUM_QUEUE);
    }

    @Test
    void aDeletedQueueKeepsTheKindItWasCreatedWith() throws Exception {
        EventExchangeStream s = stream();
        s.start();
        next();
        DeliverCallback callback = delivery();
        Map<String, Object> created =
                Map.of("name", "fresh", "vhost", "/", "arguments", Map.of("x-queue-type", "stream"));
        callback.handle("tag", message("queue.created", created));
        callback.handle("tag", message("queue.deleted", Map.of("name", "fresh", "vhost", "/")));
        BrokerEvent createdEvent = (BrokerEvent) next();
        BrokerEvent deletedEvent = (BrokerEvent) next();
        assertThat(createdEvent.subject().orElseThrow().kind()).isEqualTo(RabbitMqKinds.STREAM);
        assertThat(deletedEvent.subject().orElseThrow().kind()).isEqualTo(RabbitMqKinds.STREAM);
        callback.handle("tag", message("queue.deleted", Map.of("name", "fresh", "vhost", "/")));
        BrokerEvent forgotten = (BrokerEvent) next();
        assertThat(forgotten.subject().orElseThrow().kind()).isNotNull();
    }

    @Test
    void aMessageWithoutHeadersAndOneAboutTheDefaultExchangeAreHandled() throws Exception {
        EventExchangeStream s = stream();
        s.start();
        next();
        DeliverCallback callback = delivery();
        callback.handle(
                "tag",
                new Delivery(
                        new Envelope(1, false, "amq.rabbitmq.event", "node.up"),
                        new AMQP.BasicProperties.Builder().build(),
                        new byte[0]));
        assertThat(((BrokerEvent) next()).type()).isEqualTo("rabbitmq.node.up");
        callback.handle("tag", message("binding.created", Map.of("source_name", "", "destination_name", "q")));
        assertThat(received.poll(200, TimeUnit.MILLISECONDS)).isNull();
    }

    @Test
    void itDoesNotReportItsOwnQueueBindingOrConsumer() throws Exception {
        EventExchangeStream s = stream();
        s.start();
        next();
        DeliverCallback callback = delivery();
        callback.handle("tag", message("queue.created", Map.of("name", "amq.gen-1", "vhost", "/")));
        callback.handle(
                "tag",
                message(
                        "binding.created",
                        Map.of(
                                "source_name", "amq.rabbitmq.event",
                                "destination_name", "amq.gen-1",
                                "destination_kind", "queue",
                                "routing_key", "#",
                                "vhost", "/")));
        callback.handle("tag", message("consumer.created", Map.of("queue", "amq.gen-1", "vhost", "/")));
        assertThat(received.poll(200, TimeUnit.MILLISECONDS)).isNull();
        callback.handle("tag", message("queue.created", Map.of("name", "other", "vhost", "/")));
        assertThat(((BrokerEvent) next()).subject().orElseThrow().name()).isEqualTo("other");
    }

    @Test
    void aRefusedBindingFailsTheStartWithPermissionDeniedAndLeavesNothingOpen() throws Exception {
        ShutdownSignalException refused = new ShutdownSignalException(
                false,
                false,
                new AMQP.Channel.Close.Builder()
                        .replyCode(403)
                        .replyText("ACCESS_REFUSED")
                        .build(),
                channel);
        doThrow(new IOException(refused)).when(channel).queueBind(anyString(), anyString(), anyString());
        EventExchangeStream s = stream();
        assertThatThrownBy(s::start)
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.PERMISSION_DENIED));
        assertThat(s.state().phase()).isEqualTo(StreamState.Phase.UNAVAILABLE);
        verify(timer).shutdownNow();
    }

    @Test
    void aBrokerFailureOpeningTheConnectionPassesThrough() {
        BrokerException failure = RabbitMqErrors.invalid("no");
        when(connections.connection(eq("/"), any(Deadline.class))).thenThrow(failure);
        EventExchangeStream s = stream();
        assertThatThrownBy(s::start).isSameAs(failure);
    }

    @Test
    void aLostConnectionIsReopenedAndTheListenerToldEventsWereMissed() throws Exception {
        EventExchangeStream s = stream();
        s.start();
        next();
        ArgumentCaptor<Runnable> loss = ArgumentCaptor.forClass(Runnable.class);
        verify(connections).onLoss(eq("/"), loss.capture());
        loss.getValue().run();
        loss.getValue().run(); // a second signal while backing off schedules nothing more
        assertThat(s.state().phase()).isEqualTo(StreamState.Phase.BACKING_OFF);
        ArgumentCaptor<Runnable> retry = ArgumentCaptor.forClass(Runnable.class);
        verify(timer).schedule(retry.capture(), eq(1000L), eq(TimeUnit.MILLISECONDS));
        retry.getValue().run();
        assertThat(s.state().phase()).isEqualTo(StreamState.Phase.ACTIVE);
        Object gap = next();
        assertThat(((Gap) gap).cause()).isEqualTo(Gap.Cause.RECONNECTED);
    }

    @Test
    void aReopenThatFailsBacksOffLongerAndTriesAgain() throws Exception {
        EventExchangeStream s = stream();
        s.start();
        next();
        ArgumentCaptor<Runnable> loss = ArgumentCaptor.forClass(Runnable.class);
        verify(connections).onLoss(eq("/"), loss.capture());
        when(connections.connection(eq("/"), any(Deadline.class))).thenThrow(RabbitMqErrors.invalid("down"));
        loss.getValue().run();
        ArgumentCaptor<Runnable> retry = ArgumentCaptor.forClass(Runnable.class);
        verify(timer).schedule(retry.capture(), eq(1000L), eq(TimeUnit.MILLISECONDS));
        retry.getValue().run();
        assertThat(s.state().phase()).isEqualTo(StreamState.Phase.BACKING_OFF);
        assertThat(s.state().lastFailure()).isPresent();
        verify(timer).schedule(any(Runnable.class), eq(2000L), eq(TimeUnit.MILLISECONDS));
        assertThat(received.poll(200, TimeUnit.MILLISECONDS)).isNull();
    }

    @Test
    void aChannelShutdownOrACancelReopensToo() throws Exception {
        EventExchangeStream s = stream();
        s.start();
        next();
        ArgumentCaptor<ShutdownListener> shutdown = ArgumentCaptor.forClass(ShutdownListener.class);
        verify(channel).addShutdownListener(shutdown.capture());
        shutdown.getValue().shutdownCompleted(null);
        assertThat(s.state().phase()).isEqualTo(StreamState.Phase.BACKING_OFF);
        verify(timer).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    }

    @Test
    void closeClosesTheChannelAndStopsReopening() throws Exception {
        EventExchangeStream s = stream();
        s.start();
        ArgumentCaptor<Runnable> loss = ArgumentCaptor.forClass(Runnable.class);
        verify(connections).onLoss(eq("/"), loss.capture());
        s.close();
        verify(channel).close();
        loss.getValue().run();
        assertThat(s.state().phase()).isEqualTo(StreamState.Phase.ACTIVE);
        s.reopen();
        verify(connection).createChannel();
    }
}
