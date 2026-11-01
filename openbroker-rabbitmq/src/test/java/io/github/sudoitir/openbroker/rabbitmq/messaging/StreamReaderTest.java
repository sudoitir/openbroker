package io.github.sudoitir.openbroker.rabbitmq.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.ConsumerShutdownSignalCallback;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.AMQImpl;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageFilter;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

class StreamReaderTest {

    private static final ResourceId STREAM = ResourceId.of("rabbitmq.stream", "/", "events");
    private static final String INFO = "{\"committed_offset\":9,\"messages\":10,\"first_timestamp\":1000}";
    private static final Duration WAIT = Duration.ofSeconds(5);

    private static StreamReader reader(MessagingMocks m, ReceiverOptions options) {
        return new StreamReader(m.cluster, STREAM, m.connections, false, options, new ConcurrentHashMap<>());
    }

    private static List<String> positions(List<Message> messages) {
        return messages.stream().map(message -> message.ref().position()).toList();
    }

    @Test
    void aReceiverReadsOnFromWhereItStopped() throws Exception {
        MessagingMocks m = new MessagingMocks(MessagingMocks.WITH_DATA);
        m.info(INFO);
        m.stream(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        StreamReader reader = reader(m, ReceiverOptions.browse(Optional.empty()));

        assertThat(positions(reader.receive(4, WAIT))).containsExactly("0", "1", "2", "3");
        assertThat(positions(reader.receive(4, WAIT))).containsExactly("4", "5", "6", "7");
        assertThat(positions(reader.receive(4, WAIT))).containsExactly("8", "9");
        assertThat(reader.receive(4, WAIT)).isEmpty();

        assertThat(m.consumed).hasSize(3);
        assertThat(m.consumed.get(1)).containsEntry("x-stream-offset", 4L);
        verify(m.channel, times(10)).basicAck(anyLong(), eq(false));
        verify(m.channel, never()).basicReject(anyLong(), anyBoolean());
    }

    @Test
    void aProductionTimeBoundStopsBeforeTheFirstMessageAfterIt() throws Exception {
        MessagingMocks m = new MessagingMocks(MessagingMocks.WITH_DATA);
        m.info(INFO);
        m.stream(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        m.timeStart = 5;
        StreamReader reader = reader(
                m, ReceiverOptions.browse(Optional.empty()).producedAtOrBefore(Instant.parse("2026-01-01T00:00:00Z")));

        assertThat(positions(reader.receive(100, WAIT))).containsExactly("0", "1", "2", "3", "4");
        assertThat(reader.receive(100, WAIT)).isEmpty();

        // One probe for the bound, then the reads.
        assertThat(m.consumed.get(0).get("x-stream-offset")).isInstanceOf(Date.class);
    }

    @Test
    void aProductionTimeBoundAfterTheLastMessageReadsToTheEnd() throws Exception {
        MessagingMocks m = new MessagingMocks(MessagingMocks.WITH_DATA);
        m.info("{\"committed_offset\":1,\"first_timestamp\":1000}");
        m.stream(0, 1);
        m.timeStart = 100;
        StreamReader reader = reader(
                m, ReceiverOptions.browse(Optional.empty()).producedAtOrBefore(Instant.parse("2026-01-01T00:00:00Z")));

        assertThat(positions(reader.receive(100, WAIT))).containsExactly("0", "1");
    }

    @Test
    void aReceiverAppliesItsPortableFilter() throws Exception {
        MessagingMocks m = new MessagingMocks(MessagingMocks.WITH_DATA);
        m.info("{\"committed_offset\":1,\"first_timestamp\":1000}");
        when(m.channel.basicConsume(
                        anyString(),
                        anyBoolean(),
                        anyMap(),
                        any(DeliverCallback.class),
                        any(CancelCallback.class),
                        any(ConsumerShutdownSignalCallback.class)))
                .thenAnswer(call -> {
                    DeliverCallback deliver = call.getArgument(3);
                    deliver.handle("tag", MessagingMocks.delivery(0, Map.of("color", "red")));
                    deliver.handle("tag", MessagingMocks.delivery(1, Map.of("color", "blue")));
                    return "tag";
                });
        StreamReader reader =
                reader(m, ReceiverOptions.browse(Optional.of(MessageFilter.portable("property.color = 'blue'"))));

        assertThat(positions(reader.receive(10, WAIT))).containsExactly("1");
    }

    @Test
    void aServerCancelOrAChannelShutdownFailsTheRead() throws Exception {
        MessagingMocks m = new MessagingMocks(MessagingMocks.WITH_DATA);
        m.info(INFO);
        when(m.channel.basicConsume(
                        anyString(),
                        anyBoolean(),
                        anyMap(),
                        any(DeliverCallback.class),
                        any(CancelCallback.class),
                        any(ConsumerShutdownSignalCallback.class)))
                .thenAnswer(call -> {
                    CancelCallback cancel = call.getArgument(4);
                    cancel.handle("tag");
                    return "tag";
                })
                .thenAnswer(call -> {
                    ConsumerShutdownSignalCallback shutdown = call.getArgument(5);
                    shutdown.handleShutdownSignal(
                            "tag",
                            new ShutdownSignalException(
                                    false, false, new AMQImpl.Channel.Close(404, "NOT_FOUND", 0, 0), null));
                    return "tag";
                });
        StreamReader reader = reader(m, ReceiverOptions.browse(Optional.empty()));

        assertThatThrownBy(() -> reader.receive(5, WAIT)).isInstanceOf(BrokerException.class);
        assertThatThrownBy(() -> reader.receive(5, WAIT)).isInstanceOf(BrokerException.class);
        verify(m.channel, times(2)).close();
    }

    @Test
    void aChannelThatCannotBeOpenedFailsTheRead() throws Exception {
        MessagingMocks m = new MessagingMocks(MessagingMocks.WITH_DATA);
        m.info(INFO);
        when(m.connection.createChannel()).thenThrow(new IOException("no channel"));
        StreamReader reader = reader(m, ReceiverOptions.browse(Optional.empty()));

        assertThatThrownBy(() -> reader.receive(5, WAIT)).isInstanceOf(BrokerException.class);
    }

    @Test
    void settlingChangesNothingAndTheOtherOperationsAreRefused() throws Exception {
        MessagingMocks m = new MessagingMocks(MessagingMocks.WITH_DATA);
        StreamReader reader = reader(m, ReceiverOptions.browse(Optional.empty()));

        assertThat(reader.id()).startsWith("stream-reader-");
        assertThat(reader.state()).isEqualTo(LinkState.OPEN);
        reader.settle(null, Disposition.accept());
        for (Runnable refused : List.<Runnable>of(
                () -> reader.sender(SendTarget.destination(STREAM)), reader::commit, reader::rollback)) {
            assertThatThrownBy(refused::run)
                    .isInstanceOfSatisfying(
                            BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED));
        }
        verifyNoInteractions(m.channel, m.management);
    }

    @Test
    void aClosedReceiverReadsNothing() throws Exception {
        MessagingMocks m = new MessagingMocks(MessagingMocks.WITH_DATA);
        StreamReader reader = reader(m, ReceiverOptions.browse(Optional.empty()));

        reader.close();

        assertThat(reader.state()).isEqualTo(LinkState.CLOSED);
        assertThatThrownBy(() -> reader.receive(1, WAIT)).isInstanceOf(BrokerException.class);
        verify(m.connections, never()).close();
    }

    @Test
    void aReceiverMustReadAPositiveNumber() throws Exception {
        MessagingMocks m = new MessagingMocks(MessagingMocks.WITH_DATA);
        StreamReader reader = reader(m, ReceiverOptions.browse(Optional.empty()));

        assertThatThrownBy(() -> reader.receive(0, WAIT)).isInstanceOf(BrokerException.class);
    }

    @Test
    void aReaderOfAQueueIsRefusedAtOnce() throws Exception {
        MessagingMocks m = new MessagingMocks(MessagingMocks.WITH_DATA);

        assertThatThrownBy(() -> new StreamReader(m.cluster, ResourceId.of("rabbitmq.classic-queue", "/", "q")))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED));
    }

    @Test
    void aReaderWithItsOwnConnectionsClosesThem() throws Exception {
        MessagingMocks m = new MessagingMocks(MessagingMocks.WITH_DATA);
        StreamReader own = new StreamReader(m.cluster, STREAM);

        own.close();

        assertThat(own.state()).isEqualTo(LinkState.CLOSED);
    }
}
