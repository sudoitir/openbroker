package io.github.sudoitir.openbroker.kafka.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class SettlementTrackerTest {

    private static SettlementTracker fetched(long... offsets) {
        SettlementTracker tracker = new SettlementTracker();
        for (long offset : offsets) {
            tracker.fetched(offset);
        }
        return tracker;
    }

    @Test
    void nothingFetchedCommitsNothing() {
        assertThat(new SettlementTracker().pendingCommit()).isEmpty();
    }

    @Test
    void outOfOrderSettlementCommitsOnlyThroughTheContiguousPrefix() {
        SettlementTracker tracker = fetched(4, 5, 6, 7);
        tracker.settled(5);
        assertThat(tracker.pendingCommit()).isEmpty();
        tracker.settled(7);
        assertThat(tracker.pendingCommit()).isEmpty();
        tracker.settled(4);
        assertThat(tracker.pendingCommit()).hasValue(6);
        tracker.settled(6);
        assertThat(tracker.pendingCommit()).hasValue(8);
    }

    @Test
    void nothingIsCommittedBeforeTheFirstRecordSettles() {
        SettlementTracker tracker = fetched(3, 4);
        assertThat(tracker.pendingCommit()).isEmpty();
        tracker.settled(4);
        assertThat(tracker.pendingCommit()).isEmpty();
    }

    @Test
    void aFetchedButWaitingRecordIsNeverCommittedPast() {
        SettlementTracker tracker = fetched(0, 1, 2);
        tracker.settled(0);
        tracker.settled(2);
        assertThat(tracker.pendingCommit()).hasValue(1);
        assertThat(tracker.unsettled()).isEqualTo(1);
    }

    @Test
    void aCommitReportsOnlyWhatMovedBeyondTheLastCommit() {
        SettlementTracker tracker = fetched(0, 1);
        tracker.settled(0);
        tracker.committed(1);
        assertThat(tracker.pendingCommit()).isEmpty();
        tracker.settled(1);
        assertThat(tracker.pendingCommit()).hasValue(2);
        tracker.committed(2);
        assertThat(tracker.pendingCommit()).isEmpty();
    }

    @Test
    void gapsInOffsetsFromCompactionOrMarkersDoNotBlockTheCommit() {
        SettlementTracker tracker = fetched(10, 14, 20);
        tracker.settled(10);
        tracker.settled(14);
        assertThat(tracker.pendingCommit()).hasValue(20);
        tracker.settled(20);
        assertThat(tracker.pendingCommit()).hasValue(21);
    }

    @Test
    void settlingTwiceOrSettlingWhatWasNeverFetchedChangesNothing() {
        SettlementTracker tracker = fetched(0, 1);
        tracker.settled(0);
        tracker.settled(0);
        tracker.settled(99);
        assertThat(tracker.unsettled()).isEqualTo(1);
        assertThat(tracker.pendingCommit()).hasValue(1);
    }

    @Test
    void theCommitIfSomeWereSettledLeavesTheTrackerUnchanged() {
        SettlementTracker tracker = fetched(0, 1, 2, 3);
        tracker.settled(1);
        assertThat(tracker.pendingCommitAfter(List.of(0L, 2L))).hasValue(3);
        assertThat(tracker.pendingCommitAfter(List.of(0L))).hasValue(2);
        assertThat(tracker.pendingCommit()).isEmpty();
        assertThat(tracker.unsettled()).isEqualTo(3);
    }
}
