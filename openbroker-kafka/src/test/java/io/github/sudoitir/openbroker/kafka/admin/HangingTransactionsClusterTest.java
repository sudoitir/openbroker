package io.github.sudoitir.openbroker.kafka.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Count;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.OperationResult;
import io.github.sudoitir.openbroker.operation.Preview;
import io.github.sudoitir.openbroker.operation.Resolution;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.kafka.clients.admin.AbortTransactionResult;
import org.apache.kafka.clients.admin.AbortTransactionSpec;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeProducersResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.ListTransactionsResult;
import org.apache.kafka.clients.admin.ProducerState;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The hanging-transaction handler against an Admin client that answers as a cluster would. */
class HangingTransactionsClusterTest {

    private static final Instant NOW = Instant.parse("2026-10-29T12:00:00Z");
    private static final long LONG_AGO = NOW.toEpochMilli() - 3_600_000;
    private static final TopicPartition TP = new TopicPartition("orders", 0);

    private final Admin admin = mock(Admin.class);
    private final HangingTransactions handler = new HangingTransactions(KafkaKinds.base().stream()
            .filter(k -> k.id().equals(KafkaKinds.HANGING_TRANSACTION))
            .findFirst()
            .orElseThrow());

    private static <T> KafkaFuture<T> done(T value) {
        return KafkaFuture.completedFuture(value);
    }

    private static <T> KafkaFuture<T> failed(Throwable error) {
        KafkaFutureImpl<T> future = new KafkaFutureImpl<>();
        future.completeExceptionally(error);
        return future;
    }

    private Context context() {
        Clock clock = Clock.fixed(NOW, java.time.ZoneOffset.UTC);
        return new Context(
                null,
                null,
                admin,
                new Await(Deadline.after(Duration.ofSeconds(30), clock), clock, NodeId.of("cluster")),
                NOW,
                null);
    }

    private static ProducerState open(long producerId, long lastWrite, long startOffset) {
        return new ProducerState(producerId, 4, 9, lastWrite, OptionalInt.of(2), OptionalLong.of(startOffset));
    }

    private void producers(
            TopicPartition partition, KafkaFuture<DescribeProducersResult.PartitionProducerState> state) {
        DescribeProducersResult result = mock(DescribeProducersResult.class);
        when(result.partitionResult(partition)).thenReturn(state);
        when(admin.describeProducers(anyCollection(), any())).thenReturn(result);
    }

    private void producers(ProducerState... states) {
        producers(TP, done(new DescribeProducersResult.PartitionProducerState(List.of(states))));
    }

    private void cluster(TopicPartition partition, long maxTimeoutMs, long end, Set<Long> inProgress) {
        TopicDescription description = new TopicDescription(
                partition.topic(),
                false,
                List.of(new TopicPartitionInfo(partition.partition(), null, List.of(), List.of())));
        DescribeTopicsResult topics = mock(DescribeTopicsResult.class);
        when(topics.topicNameValues()).thenReturn(Map.of(partition.topic(), done(description)));
        when(admin.describeTopics(any(org.apache.kafka.common.TopicCollection.class), any()))
                .thenReturn(topics);
        ListTopicsResult listed = mock(ListTopicsResult.class);
        when(listed.names()).thenReturn(done(Set.of(partition.topic())));
        when(admin.listTopics(any())).thenReturn(listed);
        DescribeClusterResult described = mock(DescribeClusterResult.class);
        when(described.nodes()).thenReturn(done(List.of(new Node(1, "b1", 9092))));
        when(admin.describeCluster(any())).thenReturn(described);
        DescribeConfigsResult configs = mock(DescribeConfigsResult.class);
        ConfigResource broker = new ConfigResource(ConfigResource.Type.BROKER, "1");
        when(configs.all())
                .thenReturn(done(Map.of(
                        broker,
                        new Config(List.of(
                                new ConfigEntry("transaction.max.timeout.ms", String.valueOf(maxTimeoutMs)))))));
        when(admin.describeConfigs(anyCollection(), any())).thenReturn(configs);
        ListTransactionsResult transactions = mock(ListTransactionsResult.class);
        when(transactions.all())
                .thenReturn(done(inProgress.stream()
                        .map(id -> new TransactionListing("tx-" + id, id, TransactionState.ONGOING))
                        .toList()));
        when(admin.listTransactions(any())).thenReturn(transactions);
        ListOffsetsResult offsets = mock(ListOffsetsResult.class);
        when(offsets.all())
                .thenReturn(
                        done(Map.of(partition, new ListOffsetsResult.ListOffsetsResultInfo(end, 0, Optional.empty()))));
        when(admin.listOffsets(anyMap(), any())).thenReturn(offsets);
    }

    private Listing everything() {
        return new Listing(ResourceQuery.of(KafkaKinds.HANGING_TRANSACTION));
    }

    @BeforeEach
    void aClusterWithOneStuckProducer() {
        cluster(TP, 900_000, 250, Set.of());
    }

    @Test
    void aTransactionTheCoordinatorForgotIsListedWithWhatItHoldsBack() {
        producers(open(42, LONG_AGO, 100));

        List<Candidate> found = handler.candidates(context(), everything());
        List<Resource> resources = handler.resources(context(), found, everything());

        assertThat(found).singleElement().satisfies(c -> assertThat(c.id()).isEqualTo(HangingTransactions.id(TP, 42)));
        assertThat(resources).singleElement().satisfies(r -> {
            assertThat(r.attributes().raw(KafkaKinds.RECORDS_HELD_BACK)).contains(150L);
            assertThat(r.attributes().raw(KafkaKinds.PRODUCER_EPOCH)).contains(4L);
        });
    }

    @Test
    void aTransactionTheCoordinatorStillListsIsLeftAlone() {
        cluster(TP, 900_000, 250, Set.of(42L));
        producers(open(42, LONG_AGO, 100));

        assertThat(handler.candidates(context(), everything())).isEmpty();
    }

    @Test
    void aListingIsBoundedByItsLimitAndCanBeScopedToATopic() {
        producers(open(42, LONG_AGO, 100), open(43, LONG_AGO, 120));
        Listing one = new Listing(ResourceQuery.of(KafkaKinds.HANGING_TRANSACTION)
                .where(Map.of("topic", "orders"))
                .limit(1));

        assertThat(handler.candidates(context(), one)).hasSize(1);
        assertThat(handler.candidates(context(), everything())).hasSize(2);
    }

    @Test
    void theLongestTransactionFallsBackToKafkasDefaultWhenTheBrokerWillNotSay() {
        DescribeConfigsResult broken = mock(DescribeConfigsResult.class);
        when(broken.all()).thenReturn(failed(new UnknownTopicOrPartitionException("no")));
        when(admin.describeConfigs(anyCollection(), any())).thenReturn(broken);
        producers(open(42, NOW.toEpochMilli() - 1_000_000, 100));

        // 1000 s is past the 900 s default, so it hangs.
        assertThat(handler.candidates(context(), everything())).hasSize(1);
    }

    @Test
    void aPartitionThatCannotBeReadIsSkippedButAClusterThatCannotListProducersFails() {
        producers(TP, failed(new UnknownTopicOrPartitionException("moved")));
        assertThat(handler.candidates(context(), everything())).isEmpty();

        producers(TP, failed(new UnsupportedVersionException("old")));
        assertThatThrownBy(() -> handler.candidates(context(), everything()))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.reason()).isEqualTo(ErrorReasons.UNSUPPORTED_VERSION));
    }

    @Test
    void aTopicWithNoHangingTransactionsListsNothing() {
        producers();

        assertThat(handler.candidates(context(), everything())).isEmpty();
        assertThat(handler.resources(context(), List.of(), everything())).isEmpty();
    }

    @Test
    void thePreviewNamesTheRecordsItWouldReleaseAndTheEtagFollowsTheStartOffsetAndEpoch() {
        producers(open(42, LONG_AGO, 100));
        ResourceId id = HangingTransactions.id(TP, 42);
        Operation rollback = new Operation.Resolve(Resolution.ROLLBACK);

        Preview preview = handler.preview(context(), id, rollback, Scope.whole());
        String same = handler.etag(context(), id, rollback, Scope.whole());
        producers(open(42, LONG_AGO, 130));
        String moved = handler.etag(context(), id, rollback, Scope.whole());

        assertThat(preview.estimate().count()).isEqualTo(Count.exact(150));
        assertThat(preview.etag()).isEqualTo(same).isNotEqualTo(moved);
    }

    @Test
    void resolvingAbortsTheTransactionWithItsProducerAndEpochs() {
        producers(open(42, LONG_AGO, 100));
        AbortTransactionResult aborted = mock(AbortTransactionResult.class);
        when(aborted.all()).thenReturn(done(null));
        when(admin.abortTransaction(any())).thenReturn(aborted);

        OperationResult result = handler.execute(
                context(), HangingTransactions.id(TP, 42), new Operation.Resolve(Resolution.ROLLBACK), Scope.whole());

        assertThat(result.alreadyInState()).isFalse();
        verify(admin).abortTransaction(new AbortTransactionSpec(TP, 42, (short) 4, 2));
    }

    @Test
    void aTransactionThatIsNoLongerOpenIsRefusedByPreviewEtagAndResolve() {
        producers();
        ResourceId id = HangingTransactions.id(TP, 42);
        Operation rollback = new Operation.Resolve(Resolution.ROLLBACK);

        for (Runnable attempt : List.<Runnable>of(
                () -> handler.preview(context(), id, rollback, Scope.whole()),
                () -> handler.etag(context(), id, rollback, Scope.whole()),
                () -> handler.execute(context(), id, rollback, Scope.whole()))) {
            assertThatThrownBy(attempt::run)
                    .isInstanceOfSatisfying(
                            BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION));
        }
    }

    @Test
    void findAnswersNothingForATransactionThatIsGone() {
        producers();

        assertThat(handler.find(context(), HangingTransactions.id(TP, 42))).isEmpty();
    }
}
