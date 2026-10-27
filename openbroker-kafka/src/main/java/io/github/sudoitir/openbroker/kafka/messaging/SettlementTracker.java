package io.github.sudoitir.openbroker.kafka.messaging;

import java.util.Collection;
import java.util.OptionalLong;
import java.util.TreeSet;

/**
 * The settlement of one partition. Every offset is held from the moment it is fetched, whether it is
 * buffered, queued in a lane, paused or running, until it is settled. The commit position is the
 * lowest offset not yet settled, or one past the highest offset fetched when every fetched offset is
 * settled, so a record that is fetched but waiting is never committed past. Thread-safe: the poll
 * thread fetches and commits, and handler threads settle.
 */
final class SettlementTracker {

    private final TreeSet<Long> unsettled = new TreeSet<>();
    private long firstFetched = -1;
    private long highestFetched = -1;
    private long committed = -1;

    /** Holds {@code offset} until it is settled. */
    synchronized void fetched(long offset) {
        if (firstFetched < 0) {
            firstFetched = offset;
        }
        unsettled.add(offset);
        highestFetched = Math.max(highestFetched, offset);
    }

    /** Releases {@code offset}; settling twice, or an offset never fetched, changes nothing. */
    synchronized void settled(long offset) {
        unsettled.remove(offset);
    }

    /** The offset to commit, when it is beyond the last commit. */
    synchronized OptionalLong pendingCommit() {
        if (highestFetched < 0) {
            return OptionalLong.empty();
        }
        long position = unsettled.isEmpty() ? highestFetched + 1 : unsettled.first();
        return progressed(position);
    }

    /** The offset to commit if {@code offsets} were settled too, without settling them. */
    synchronized OptionalLong pendingCommitAfter(Collection<Long> offsets) {
        if (highestFetched < 0) {
            return OptionalLong.empty();
        }
        long position = highestFetched + 1;
        for (long offset : unsettled) {
            if (!offsets.contains(offset)) {
                position = offset;
                break;
            }
        }
        return progressed(position);
    }

    /** The position, when it is past where the consumer started and past the last commit: a commit of the start position says nothing. */
    private OptionalLong progressed(long position) {
        return position > Math.max(committed, firstFetched) ? OptionalLong.of(position) : OptionalLong.empty();
    }

    /** Records that the commit of {@code position} reached the cluster. */
    synchronized void committed(long position) {
        committed = Math.max(committed, position);
    }

    /** The number of offsets fetched and not yet settled. */
    synchronized int unsettled() {
        return unsettled.size();
    }
}
