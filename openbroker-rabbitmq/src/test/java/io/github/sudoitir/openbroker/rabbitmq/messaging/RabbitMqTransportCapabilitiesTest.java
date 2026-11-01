package io.github.sudoitir.openbroker.rabbitmq.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verifyNoInteractions;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.DeduplicationScope;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.OrderingScope;
import io.github.sudoitir.openbroker.messaging.PrefetchBound;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.RetryPolicy;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.messaging.SettlementScope;
import io.github.sudoitir.openbroker.messaging.SubscriptionCapabilities;
import io.github.sudoitir.openbroker.messaging.SystemRetry;
import io.github.sudoitir.openbroker.messaging.TransportCapabilities;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqKinds;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class RabbitMqTransportCapabilitiesTest {

    private final MessagingFixtures fixtures = new MessagingFixtures();
    private final RabbitMqTransport transport = new RabbitMqTransport(fixtures.cluster, fixtures.connections);
    private final ResourceId stream = ResourceId.of(RabbitMqKinds.STREAM.value(), "/", "s");
    private final ResourceId classic = ResourceId.of(RabbitMqKinds.CLASSIC_QUEUE.value(), "/", "c");

    @AfterEach
    void close() {
        transport.close();
    }

    private static void refused(Runnable call, Status status) {
        assertThatThrownBy(call::run)
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(status));
    }

    @Test
    void theCapabilitiesAreStaticAndNeedNoIo() {
        TransportCapabilities c = transport.capabilities();
        assertThat(c.guarantees()).containsExactlyInAnyOrder(Guarantee.AT_MOST_ONCE, Guarantee.AT_LEAST_ONCE);
        assertThat(c.deduplication()).isEqualTo(DeduplicationScope.NONE);
        assertThat(c.orderings()).containsExactly(OrderingScope.NONE);
        assertThat(c.prefetchBounds()).containsExactly(PrefetchBound.MESSAGES);
        assertThat(c.failover()).isTrue();
        assertThat(c.subscriptions().keySet())
                .containsExactlyInAnyOrder(RabbitMqKinds.CLASSIC_QUEUE, RabbitMqKinds.QUORUM_QUEUE);
        assertThat(c.subscription(RabbitMqKinds.CLASSIC_QUEUE))
                .isEqualTo(new SubscriptionCapabilities(
                        Set.of(OrderingScope.NONE), SettlementScope.MESSAGE, false, SystemRetry.NONE, false));
        assertThat(c.subscription(RabbitMqKinds.QUORUM_QUEUE))
                .isEqualTo(new SubscriptionCapabilities(
                        Set.of(OrderingScope.NONE),
                        SettlementScope.MESSAGE,
                        false,
                        new SystemRetry(false, true, true),
                        true));
        verifyNoInteractions(fixtures.connections);
    }

    @Test
    void aStreamHasNoConsumingReceiverOrProcessor() {
        refused(() -> transport.receiver(stream, Scope.whole(), ReceiverOptions.consume()), Status.UNIMPLEMENTED);
        refused(
                () -> transport.process(stream, Scope.whole(), ProcessorOptions.defaults(), d -> null),
                Status.UNIMPLEMENTED);
        verifyNoInteractions(fixtures.connections);
    }

    @Test
    void aQueueCannotBeBrowsed() {
        refused(
                () -> transport.receiver(classic, Scope.whole(), ReceiverOptions.browse(Optional.empty())),
                Status.UNIMPLEMENTED);
        verifyNoInteractions(fixtures.connections);
    }

    @Test
    void aConsumingReceiverTakesNoBoundOrFilter() {
        refused(
                () -> transport.receiver(
                        classic, Scope.whole(), ReceiverOptions.consume().producedAtOrBefore(Instant.now())),
                Status.UNIMPLEMENTED);
        refused(
                () -> transport.receiver(
                        ResourceId.of("rabbitmq.exchange", "/", "x"), Scope.whole(), ReceiverOptions.consume()),
                Status.INVALID_ARGUMENT);
        verifyNoInteractions(fixtures.connections);
    }

    @Test
    void whatIsNotOfferedIsRefusedBeforeAnyIo() {
        SendTarget target = SendTarget.subscription(classic);
        refused(
                () -> transport.sender(
                        target, Scope.whole(), SenderOptions.defaults().withGuarantee(Guarantee.TRANSACTED)),
                Status.UNIMPLEMENTED);
        refused(
                () -> transport.sender(
                        target, Scope.whole(), SenderOptions.defaults().withDeduplication(DeduplicationScope.WINDOW)),
                Status.UNIMPLEMENTED);
        refused(
                () -> transport.sender(
                        target, Scope.whole(), SenderOptions.defaults().withOrdering(OrderingScope.GROUP)),
                Status.UNIMPLEMENTED);
        refused(
                () -> transport.process(
                        classic,
                        Scope.whole(),
                        ProcessorOptions.defaults().withOrdering(OrderingScope.GROUP),
                        d -> null),
                Status.UNIMPLEMENTED);
        refused(
                () -> transport.process(
                        classic,
                        Scope.whole(),
                        new ProcessorOptions(
                                ReceiverOptions.consume().withGuarantee(Guarantee.AT_MOST_ONCE),
                                1,
                                OrderingScope.NONE,
                                RetryPolicy.defaults(),
                                Duration.ZERO),
                        d -> null),
                Status.UNIMPLEMENTED);
        refused(
                () -> transport.process(
                        classic, Scope.whole(), ProcessorOptions.defaults().withRetry(RetryPolicy.system()), d -> null),
                Status.UNIMPLEMENTED);
        verifyNoInteractions(fixtures.connections);
    }

    @Test
    void aSenderTargetNeedsAnExchangeOrAQueue() {
        refused(
                () -> transport.sender(
                        SendTarget.destination(ResourceId.of("rabbitmq.vhost", "/")),
                        Scope.whole(),
                        SenderOptions.defaults()),
                Status.INVALID_ARGUMENT);
    }

    @Test
    void watchersAreRemovedWhenClosed() throws Exception {
        AutoCloseable watch = transport.watch(event -> {});
        watch.close();
        assertThat(transport.capabilities()).isNotNull();
    }
}
