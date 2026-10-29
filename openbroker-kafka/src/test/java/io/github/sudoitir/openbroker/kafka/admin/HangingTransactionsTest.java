package io.github.sudoitir.openbroker.kafka.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.Resolution;
import java.time.Instant;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.kafka.clients.admin.ProducerState;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class HangingTransactionsTest {

    private static final long NOW = 10_000_000;
    private static final long MAX = 900_000;

    private static ProducerState producer(long id, long lastWrite, OptionalLong open) {
        return new ProducerState(id, 3, 7, lastWrite, OptionalInt.of(5), open);
    }

    @Test
    void anOpenTransactionPastTheLongestOneThatTheCoordinatorDoesNotKnowHangs() {
        assertThat(HangingTransactions.hangs(producer(42, NOW - MAX - 1, OptionalLong.of(100)), NOW, MAX, Set.of()))
                .isTrue();
    }

    @Test
    void aTransactionTheCoordinatorStillListsHealsItself() {
        assertThat(HangingTransactions.hangs(producer(42, NOW - MAX - 1, OptionalLong.of(100)), NOW, MAX, Set.of(42L)))
                .isFalse();
    }

    @Test
    void aYoungTransactionHasNotHungYet() {
        assertThat(HangingTransactions.hangs(producer(42, NOW - MAX, OptionalLong.of(100)), NOW, MAX, Set.of()))
                .isFalse();
        assertThat(HangingTransactions.hangs(producer(42, NOW - 1000, OptionalLong.of(100)), NOW, MAX, Set.of()))
                .isFalse();
    }

    @Test
    void aProducerWithNoOpenTransactionNeverHangs() {
        assertThat(HangingTransactions.hangs(producer(42, 0, OptionalLong.empty()), NOW, MAX, Set.of()))
                .isFalse();
    }

    @Test
    void theCoordinatorsViewOfOneProducerDoesNotClearAnother() {
        assertThat(HangingTransactions.hangs(producer(43, NOW - MAX - 1, OptionalLong.of(100)), NOW, MAX, Set.of(42L)))
                .isTrue();
    }

    private static final Context CONTEXT = new Context(null, null, null, null, Instant.ofEpochMilli(NOW), null);

    private static HangingTransactions handler() {
        return new HangingTransactions(KafkaKinds.base().stream()
                .filter(k -> k.id().equals(KafkaKinds.HANGING_TRANSACTION))
                .findFirst()
                .orElseThrow());
    }

    @Test
    void aHangingTransactionNamesItsPartitionProducerAndHowLongItHasBeenOpen() {
        ProducerState p = new ProducerState(42, 3, 7, NOW - 60_000, OptionalInt.of(5), OptionalLong.of(100));

        Attributes a =
                HangingTransactions.attributes(CONTEXT, new HangingTransactions.Open(new TopicPartition("t", 2), p));

        assertThat(a.raw(KafkaKinds.HANGING_TOPIC)).contains("t");
        assertThat(a.raw(KafkaKinds.HANGING_PARTITION)).contains(2L);
        assertThat(a.raw(KafkaKinds.PRODUCER_ID)).contains(42L);
        assertThat(a.raw(KafkaKinds.PRODUCER_EPOCH)).contains(3L);
        assertThat(a.raw(KafkaKinds.COORDINATOR_EPOCH)).contains(5L);
        assertThat(a.raw(KafkaKinds.TRANSACTION_START_OFFSET)).contains(100L);
        assertThat(a.raw(KafkaKinds.HANGING_AGE_MS)).contains(60_000L);
    }

    @Test
    void theIdIsTheTopicPartitionAndProducer() {
        assertThat(HangingTransactions.id(new TopicPartition("t", 2), 42).path().segments())
                .containsExactly("t", "2", "42");
    }

    @Test
    void theKindIsListedByTopicOnly() {
        assertThat(handler().selectors()).containsExactly("topic");
        assertThat(handler().kind().id()).isEqualTo(KafkaKinds.HANGING_TRANSACTION);
    }

    @Test
    void onlyARollbackIsOffered() {
        Operation commit = new Operation.Resolve(Resolution.COMMIT);

        assertThatThrownBy(() -> handler()
                        .preview(CONTEXT, HangingTransactions.id(new TopicPartition("t", 0), 1), commit, Scope.whole()))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED));
        assertThatThrownBy(() -> handler()
                        .execute(CONTEXT, HangingTransactions.id(new TopicPartition("t", 0), 1), commit, Scope.whole()))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED));
    }

    @Test
    void anythingButAResolutionIsRefused() {
        ResourceId id = HangingTransactions.id(new TopicPartition("t", 0), 1);

        assertThatThrownBy(() -> handler().preview(CONTEXT, id, Operation.purge(), Scope.whole()))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED));
        assertThatThrownBy(() -> handler().execute(CONTEXT, id, Operation.purge(), Scope.whole()))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED));
    }

    @Test
    void aMalformedIdIsRefusedBeforeTheClusterIsAsked() {
        Operation rollback = new Operation.Resolve(Resolution.ROLLBACK);
        ResourceId twoParts = new ResourceId(KafkaKinds.HANGING_TRANSACTION, ResourcePath.of("t", "0"));
        ResourceId notNumbers = new ResourceId(KafkaKinds.HANGING_TRANSACTION, ResourcePath.of("t", "x", "y"));

        for (ResourceId bad : new ResourceId[] {twoParts, notNumbers}) {
            assertThatThrownBy(() -> handler().execute(CONTEXT, bad, rollback, Scope.whole()))
                    .isInstanceOfSatisfying(
                            BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT));
        }
    }
}
