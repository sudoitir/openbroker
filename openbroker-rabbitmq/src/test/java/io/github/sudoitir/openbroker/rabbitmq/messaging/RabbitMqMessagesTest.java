package io.github.sudoitir.openbroker.rabbitmq.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.ConsumerShutdownSignalCallback;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Return;
import com.rabbitmq.client.ReturnCallback;
import com.rabbitmq.client.impl.AMQImpl;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.BrowsePage;
import io.github.sudoitir.openbroker.messaging.BrowseRequest;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.messaging.Cursor;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageField;
import io.github.sudoitir.openbroker.messaging.MessageFilter;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.Position;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.model.PartitionId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;
import io.github.sudoitir.openbroker.rabbitmq.amqp.StreamOffsets;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class RabbitMqMessagesTest {

    private static final ResourceId STREAM = ResourceId.of("rabbitmq.stream", "/", "events");
    private static final ResourceId CLASSIC = ResourceId.of("rabbitmq.classic-queue", "/", "orders");
    private static final ResourceId QUORUM = ResourceId.of("rabbitmq.quorum-queue", "/", "orders");
    private static final String INFO = "{\"committed_offset\":9,\"messages\":10,\"first_timestamp\":1000}";
    private static final OutboundMessage HELLO = OutboundMessage.of(MessageBody.of("hello".getBytes()));

    private static BrowseRequest request(Position position, int limit) {
        return new BrowseRequest(position, limit, Optional.empty(), 1024);
    }

    private static BrokerException failure(ThrowingCallable call) {
        return (BrokerException) catchThrowable(call);
    }

    private static MessagingMocks mocks(ClusterSpec spec) throws IOException {
        return new MessagingMocks(spec);
    }

    @Test
    void browsingAQueueIsRefusedBeforeAnyChannelOrRequest() throws IOException {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        for (ResourceId queue : new ResourceId[] {CLASSIC, QUORUM}) {
            BrokerException e = failure(() -> messages.browse(queue, Scope.whole(), request(Position.earliest(), 5)));

            assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED);
            assertThat(e.info().reason()).isEqualTo(ErrorReasons.NOT_SUPPORTED);
            assertThat(e.getMessage()).contains("Browse a stream");
        }
        verifyNoInteractions(m.connections, m.connection, m.channel, m.management);
    }

    @Test
    void browsingSomethingElseIsInvalid() throws IOException {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        assertThat(failure(() -> messages.browse(
                                ResourceId.of("rabbitmq.exchange", "/", "x"),
                                Scope.whole(),
                                request(Position.earliest(), 1)))
                        .status())
                .isEqualTo(Status.INVALID_ARGUMENT);
        assertThat(failure(() -> messages.browse(
                                ResourceId.of("rabbitmq.stream", "/"), Scope.whole(), request(Position.earliest(), 1)))
                        .status())
                .isEqualTo(Status.INVALID_ARGUMENT);
        verifyNoInteractions(m.channel, m.management);
    }

    @Test
    void aStreamIsBrowsedInPagesByOffset() throws Exception {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        m.info(INFO);
        m.stream(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        BrowsePage first = messages.browse(STREAM, Scope.whole(), request(Position.earliest(), 3));

        assertThat(first.messages()).extracting(msg -> msg.ref().position()).containsExactly("0", "1", "2");
        assertThat(first.messages().get(0).ref().source()).isEqualTo(STREAM);
        assertThat(first.messages().get(0).ref().shard()).isEqualTo(StreamOffsets.PARTITION);
        assertThat(m.consumed.get(0)).containsEntry("x-stream-offset", "first");
        assertThat(first.total().count()).isEqualTo(Count.estimate(10));
        verify(m.channel).basicQos(3);
        verify(m.channel, times(3)).basicAck(anyLong(), eq(false));
        verify(m.channel).basicCancel("tag");
        verify(m.channel).close();
        verify(m.channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
        verify(m.channel, never()).basicReject(anyLong(), anyBoolean());
        Cursor cursor = first.next().orElseThrow();
        assertThat(StreamOffsets.cursorOffset(cursor)).isEqualTo(2);

        BrowsePage second = messages.browse(STREAM, Scope.whole(), request(Position.after(cursor), 3));

        assertThat(second.messages()).extracting(msg -> msg.ref().position()).containsExactly("3", "4", "5");
        assertThat(m.consumed.get(1)).containsEntry("x-stream-offset", 3L);
        assertThat(StreamOffsets.cursorOffset(second.next().orElseThrow())).isEqualTo(5);
    }

    @Test
    void thePageEndsAtTheCommittedOffsetWithoutACursor() throws Exception {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        m.info("{\"committed_offset\":2,\"messages\":3,\"first_timestamp\":1000}");
        m.stream(0, 1, 2, 3, 4);
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        BrowsePage page = messages.browse(STREAM, Scope.whole(), request(Position.earliest(), 10));

        assertThat(page.messages()).hasSize(3);
        assertThat(page.next()).isEmpty();
        assertThat(page.total().count()).isEqualTo(Count.estimate(3));
    }

    @Test
    void anExplicitOffsetStartsThere() throws Exception {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        m.info(INFO);
        m.stream(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        BrowsePage page = messages.browse(
                STREAM, Scope.whole(), request(new Position.AtOffsets(Map.of(StreamOffsets.PARTITION, 7L)), 10));

        assertThat(m.consumed.get(0)).containsEntry("x-stream-offset", 7L);
        assertThat(page.messages()).extracting(msg -> msg.ref().position()).containsExactly("7", "8", "9");
    }

    @Test
    void anOffsetThatTheStreamNoLongerRetainsIsOutOfRange() throws Exception {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        m.info(INFO);
        m.stream(4, 5);
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        BrokerException dropped = failure(() -> messages.browse(
                STREAM, Scope.whole(), request(new Position.AtOffsets(Map.of(StreamOffsets.PARTITION, 1L)), 10)));
        BrokerException beyond = failure(() -> messages.browse(
                STREAM, Scope.whole(), request(new Position.AtOffsets(Map.of(StreamOffsets.PARTITION, 50L)), 10)));

        assertThat(dropped.status()).isEqualTo(Status.OUT_OF_RANGE);
        assertThat(beyond.status()).isEqualTo(Status.OUT_OF_RANGE);
        assertThat(beyond.getMessage()).contains("50");
    }

    @Test
    void aCursorOfAnotherIncarnationOfTheStreamIsRefused() throws Exception {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        m.info("{\"committed_offset\":9,\"first_timestamp\":2000}");
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);
        Cursor stale = StreamOffsets.cursor(3, Instant.ofEpochMilli(1000));

        BrokerException e = failure(() -> messages.browse(STREAM, Scope.whole(), request(Position.after(stale), 5)));

        assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
        verify(m.channel, never()).basicQos(any(Integer.class));
    }

    @Test
    void readingFromTheEndTakesNoChannel() throws Exception {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        m.info(INFO);
        m.stream(0, 1);
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);
        Cursor atEnd = StreamOffsets.cursor(9, Instant.ofEpochMilli(1000));

        BrowsePage latest = messages.browse(STREAM, Scope.whole(), request(Position.latest(), 5));
        BrowsePage after = messages.browse(STREAM, Scope.whole(), request(Position.after(atEnd), 5));

        assertThat(latest.messages()).isEmpty();
        assertThat(latest.next()).isEmpty();
        assertThat(after.messages()).isEmpty();
        assertThat(m.consumed).isEmpty();
        verifyNoInteractions(m.channel);
        assertThat(latest.total().count()).isEqualTo(Count.estimate(10));
    }

    @Test
    void aPortableFilterDropsMessagesButStillMovesTheCursor() throws Exception {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        m.info(INFO);
        when(m.channel.basicConsume(
                        anyString(),
                        anyBoolean(),
                        any(Map.class),
                        any(DeliverCallback.class),
                        any(CancelCallback.class),
                        any(ConsumerShutdownSignalCallback.class)))
                .thenAnswer(call -> {
                    DeliverCallback deliver = call.getArgument(3);
                    deliver.handle("tag", MessagingMocks.delivery(0, Map.of("color", "red")));
                    deliver.handle("tag", MessagingMocks.delivery(1, Map.of("color", "blue")));
                    deliver.handle("tag", MessagingMocks.delivery(2, Map.of("color", "red")));
                    return "tag";
                });
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        BrowsePage page = messages.browse(
                STREAM,
                Scope.whole(),
                new BrowseRequest(
                        Position.earliest(), 2, Optional.of(MessageFilter.portable("property.color = 'red'")), 1024));

        assertThat(page.messages()).extracting(msg -> msg.ref().position()).containsExactly("0", "2");
        verify(m.channel, times(3)).basicAck(anyLong(), eq(false));
        assertThat(StreamOffsets.cursorOffset(page.next().orElseThrow())).isEqualTo(2);
    }

    @Test
    void aNativeFilterIsRefused() throws IOException {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        BrokerException e = failure(() -> messages.browse(
                STREAM,
                Scope.whole(),
                new BrowseRequest(Position.earliest(), 2, Optional.of(new MessageFilter("sql", "a = 1")), 1)));

        assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT);
        verifyNoInteractions(m.channel);
    }

    @Test
    void aStreamOfUnknownEndIsBrowsedUntilItGoesQuiet() throws Exception {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        m.info("{}");
        m.stream(0, 1);
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        BrowsePage page = messages.browse(STREAM, Scope.whole(), request(Position.earliest(), 2));

        assertThat(page.messages()).hasSize(2);
        assertThat(page.next()).isPresent();
        assertThat(page.total().count()).isEqualTo(Count.estimate(0));
    }

    @Test
    void anEmptyStreamHasNothingToBrowseOrCount() throws Exception {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        m.info("{\"committed_offset\":-1,\"messages\":0,\"first_timestamp\":0}");
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        BrowsePage page = messages.browse(STREAM, Scope.whole(), request(Position.earliest(), 2));

        assertThat(page.messages()).isEmpty();
        assertThat(page.total().count()).isEqualTo(Count.estimate(0));
    }

    // Count.

    @Test
    void aQueueWithNothingUnacknowledgedHasAnExactCount() throws Exception {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        m.info("{\"messages_unacknowledged\":0}");
        when(m.channel.queueDeclarePassive("orders")).thenReturn(new AMQImpl.Queue.DeclareOk("orders", 7, 0));
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        assertThat(messages.count(CLASSIC, Scope.whole(), Optional.empty())
                        .value()
                        .count())
                .isEqualTo(Count.exact(7));
    }

    @Test
    void aQueueWithUnacknowledgedMessagesHasAnEstimate() throws Exception {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        m.info("{\"messages_unacknowledged\":4}");
        when(m.channel.queueDeclarePassive("orders")).thenReturn(new AMQImpl.Queue.DeclareOk("orders", 7, 0));
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        assertThat(messages.count(QUORUM, Scope.whole(), Optional.empty())
                        .value()
                        .count())
                .isEqualTo(Count.estimate(11));
    }

    @Test
    void aQueueManagementCannotReadHasAnEstimateOfItsReadyMessages() throws Exception {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        when(m.management.get(anyString(), any(), any(), anyString()))
                .thenThrow(RabbitMqErrors.fromStatus(503, "read"));
        when(m.channel.queueDeclarePassive("orders")).thenReturn(new AMQImpl.Queue.DeclareOk("orders", 7, 0));
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        assertThat(messages.count(CLASSIC, Scope.whole(), Optional.empty())
                        .value()
                        .count())
                .isEqualTo(Count.estimate(7));
    }

    @Test
    void withoutADataEndpointTheCountIsManagementsSample() throws Exception {
        MessagingMocks m = mocks(MessagingMocks.WITHOUT_DATA);
        m.info("{\"messages\":12}");
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        assertThat(messages.count(CLASSIC, Scope.whole(), Optional.empty())
                        .value()
                        .count())
                .isEqualTo(Count.estimate(12));
        verifyNoInteractions(m.channel);
    }

    @Test
    void aStreamsCountIsManagementsSampledCountAsAnEstimate() throws Exception {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        m.info("{\"messages\":70}");
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        assertThat(messages.count(STREAM, Scope.whole(), Optional.empty())
                        .value()
                        .count())
                .isEqualTo(Count.estimate(70));
        verifyNoInteractions(m.channel);
    }

    @Test
    void whereTheFirstOffsetIsKnownTheSampledCountHintsTheEnd() throws Exception {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        m.info("{\"messages\":3,\"first_timestamp\":1000}");
        m.stream(0, 1, 2);
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        BrowsePage first = messages.browse(STREAM, Scope.whole(), request(Position.earliest(), 10));
        BrowsePage second = messages.browse(STREAM, Scope.whole(), request(Position.earliest(), 10));

        assertThat(first.messages()).hasSize(3);
        assertThat(second.messages()).hasSize(3);
        assertThat(second.next()).isEmpty();
    }

    @Test
    void aCountByAFilterOrAFieldOrABulkActionIsRefused() throws IOException {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        BrokerException byFilter = failure(
                () -> messages.count(CLASSIC, Scope.whole(), Optional.of(MessageFilter.portable("header.id = 'a'"))));
        BrokerException byField = failure(() -> messages.countBy(CLASSIC, Scope.whole(), MessageField.header("type")));
        BrokerException bulk = failure(() -> messages.bulk(CLASSIC, Scope.whole(), null, null));

        for (BrokerException e : new BrokerException[] {byFilter, byField, bulk}) {
            assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED);
            assertThat(e.info().reason()).isEqualTo(ErrorReasons.NOT_SUPPORTED);
        }
        assertThat(byFilter.getMessage()).contains("count by a filter");
        verifyNoInteractions(m.channel, m.management);
    }

    @Test
    void countingSomethingElseIsInvalid() throws IOException {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        assertThat(failure(() -> messages.count(
                                ResourceId.of("rabbitmq.exchange", "/", "x"), Scope.whole(), Optional.empty()))
                        .status())
                .isEqualTo(Status.INVALID_ARGUMENT);
        assertThat(failure(() -> messages.count(ResourceId.of("rabbitmq.stream", "/"), Scope.whole(), Optional.empty()))
                        .status())
                .isEqualTo(Status.INVALID_ARGUMENT);
    }

    // Send.

    @Test
    void aMessageIsPublishedMandatoryAndConfirmed() throws Exception {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        when(m.channel.waitForConfirms(anyLong())).thenReturn(true);
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        SendReceipt receipt = messages.send(
                new SendTarget.ToDestination(
                        ResourceId.of("rabbitmq.exchange", "/", "orders"), Optional.of("eu"), Optional.empty()),
                Scope.whole(),
                HELLO);

        assertThat(receipt.resent()).isFalse();
        verify(m.channel).exchangeDeclarePassive("orders");
        verify(m.channel).confirmSelect();
        verify(m.channel).basicPublish(eq("orders"), eq("eu"), eq(true), any(AMQP.BasicProperties.class), any());
        verify(m.channel, times(2)).close();
    }

    @Test
    void aMessageToASubscriptionGoesThroughTheDefaultExchange() throws Exception {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        when(m.channel.waitForConfirms(anyLong())).thenReturn(true);
        when(m.channel.queueDeclarePassive("orders")).thenReturn(new AMQImpl.Queue.DeclareOk("orders", 0, 0));
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        messages.send(SendTarget.subscription(CLASSIC), Scope.whole(), HELLO);
        messages.send(
                SendTarget.destination(ResourceId.of("rabbitmq.exchange", "/", "amq.default")), Scope.whole(), HELLO);

        verify(m.channel).basicPublish(eq(""), eq("orders"), eq(true), any(AMQP.BasicProperties.class), any());
        verify(m.channel).basicPublish(eq(""), eq(""), eq(true), any(AMQP.BasicProperties.class), any());
        verify(m.channel, never()).exchangeDeclarePassive(anyString());
    }

    @Test
    void aSendToAQueueByDestinationDeclaresTheQueue() throws Exception {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        when(m.channel.waitForConfirms(anyLong())).thenReturn(true);
        when(m.channel.queueDeclarePassive("events")).thenReturn(new AMQImpl.Queue.DeclareOk("events", 0, 0));
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        messages.send(SendTarget.destination(STREAM), Scope.whole(), HELLO);

        verify(m.channel).queueDeclarePassive("events");
    }

    @Test
    void aNegativeConfirmFailsTheSendAsFull() throws Exception {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        when(m.channel.queueDeclarePassive("orders")).thenReturn(new AMQImpl.Queue.DeclareOk("orders", 0, 0));
        when(m.channel.waitForConfirms(anyLong())).thenReturn(false);
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        BrokerException e = failure(() -> messages.send(SendTarget.subscription(CLASSIC), Scope.whole(), HELLO));

        assertThat(e.status()).isEqualTo(Status.RESOURCE_EXHAUSTED);
        assertThat(e.info().reason()).isEqualTo(ErrorReasons.DESTINATION_FULL);
    }

    @Test
    void aMessageRoutedNowhereFailsNotRouted() throws Exception {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        ArgumentCaptor<ReturnCallback> listener = ArgumentCaptor.forClass(ReturnCallback.class);
        when(m.channel.waitForConfirms(anyLong())).thenAnswer(call -> {
            verify(m.channel).addReturnListener(listener.capture());
            listener.getValue().handle(new Return(312, "NO_ROUTE", "orders", "eu", null, new byte[0]));
            return true;
        });
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        BrokerException e = failure(() -> messages.send(
                new SendTarget.ToDestination(
                        ResourceId.of("rabbitmq.exchange", "/", "orders"), Optional.of("eu"), Optional.empty()),
                Scope.whole(),
                HELLO));

        assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
        assertThat(e.info().reason()).isEqualTo(ErrorReasons.NOT_ROUTED);
    }

    @Test
    void aConfirmThatDoesNotComeIsInDoubt() throws Exception {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        when(m.channel.queueDeclarePassive("orders")).thenReturn(new AMQImpl.Queue.DeclareOk("orders", 0, 0));
        when(m.channel.waitForConfirms(anyLong())).thenThrow(new TimeoutException());
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        BrokerException e = failure(() -> messages.send(SendTarget.subscription(CLASSIC), Scope.whole(), HELLO));

        assertThat(e.status()).isEqualTo(Status.DEADLINE_EXCEEDED);
    }

    @Test
    void aBrokenChannelFailsTheSend() throws Exception {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        when(m.channel.queueDeclarePassive("orders")).thenReturn(new AMQImpl.Queue.DeclareOk("orders", 0, 0));
        when(m.channel.waitForConfirms(anyLong())).thenReturn(true);
        doThrow(new IOException("gone"))
                .when(m.channel)
                .basicPublish(anyString(), anyString(), anyBoolean(), any(AMQP.BasicProperties.class), any());
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        SendTarget target = SendTarget.subscription(CLASSIC);
        Scope whole = Scope.whole();
        assertThatThrownBy(() -> messages.send(target, whole, HELLO)).isInstanceOf(BrokerException.class);
    }

    @Test
    void aSendToAPartitionOrAWrongKindIsInvalid() throws IOException {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);
        RabbitMqMessages messages = new RabbitMqMessages(m.cluster, m.connections);

        BrokerException partition = failure(() -> messages.send(
                new SendTarget.ToDestination(
                        ResourceId.of("rabbitmq.exchange", "/", "orders"),
                        Optional.empty(),
                        Optional.of(PartitionId.of("1"))),
                Scope.whole(),
                HELLO));
        BrokerException kind = failure(() ->
                messages.send(SendTarget.destination(ResourceId.of("rabbitmq.vhost", "/")), Scope.whole(), HELLO));
        BrokerException subscription = failure(() -> messages.send(
                SendTarget.subscription(ResourceId.of("rabbitmq.exchange", "/", "x")), Scope.whole(), HELLO));
        BrokerException unnamed = failure(() ->
                messages.send(SendTarget.destination(ResourceId.of("rabbitmq.exchange", "x")), Scope.whole(), HELLO));

        for (BrokerException e : new BrokerException[] {partition, kind, subscription, unnamed}) {
            assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT);
        }
        verifyNoInteractions(m.channel);
    }

    @Test
    void closingClosesTheConnections() throws IOException {
        MessagingMocks m = mocks(MessagingMocks.WITH_DATA);

        new RabbitMqMessages(m.cluster, m.connections).close();

        verify(m.connections).close();
    }
}
