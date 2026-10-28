package io.github.sudoitir.openbroker.kafka.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.kafka.clients.admin.ProducerState;
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
}
