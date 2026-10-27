package io.github.sudoitir.openbroker.support.messaging;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.DeduplicationScope;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import io.github.sudoitir.openbroker.messaging.Observation;
import io.github.sudoitir.openbroker.messaging.Offer;
import io.github.sudoitir.openbroker.messaging.OperationInfo;
import io.github.sudoitir.openbroker.messaging.OrderingScope;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.support.Deadlines;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.random.RandomGenerator;
import org.jspecify.annotations.Nullable;

/**
 * The bounded send window of one link. Messages that are accepted but not yet confirmed are limited
 * by count and by body bytes ({@link SenderOptions#buffer()}). One writer virtual thread hands them to
 * the driver's {@link Writer} in order; the driver confirms each when the system does. Receipts
 * complete on a virtual-thread executor, never on the thread that confirmed.
 *
 * <p>When the link is lost ({@link #suspend}) the pipeline keeps accepting until the window is full.
 * After the link is reopened ({@link #resume}) unconfirmed {@code AT_LEAST_ONCE} messages are written
 * again in their original order and their receipts say {@code resent}. {@code AT_MOST_ONCE} and
 * {@code TRANSACTED} messages are never written again: their receipts fail at once. If the link
 * cannot be reopened ({@link #fail}) every pending receipt fails and nothing is written again.
 */
public final class SendPipeline implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(SendPipeline.class.getName());

    /** The wire of a link. Called from the pipeline's one writer thread. */
    public interface Writer {

        /**
         * Hands one message to the system and returns without waiting for the answer; the answer goes
         * to {@code confirmation}, from any thread. Throw a {@code NOT_SENT} {@code UNAVAILABLE}
         * {@link BrokerException} when the link is down: the message is kept for the recovery. Any
         * other failure ends that message's receipt. A failure the system reports for one message is
         * {@link Confirmation#failed}; a lost link is no confirmation at all.
         */
        void write(OutboundMessage message, Confirmation confirmation) throws BrokerException;
    }

    /** The answer for one written message. A repeated or stale answer, such as one from before a recovery, is ignored. */
    public interface Confirmation {

        /** The system holds the message as the guarantee demands. */
        void confirmed(SendReceipt receipt);

        /** The system refused the message, or its answer says it did not take effect. */
        void failed(BrokerException failure);
    }

    private enum Phase {
        RUNNING,
        SUSPENDED,
        FAILED,
        CLOSED
    }

    private static final class Entry {
        final OutboundMessage message;
        final long size;
        final @Nullable Observation observation;
        final CompletableFuture<SendReceipt> receipt = new CompletableFuture<>();
        int generation;
        boolean resent;

        Entry(OutboundMessage message, long size, @Nullable Observation observation) {
            this.message = message;
            this.size = size;
            this.observation = observation;
        }
    }

    private final NodeId node;
    private final SenderOptions options;
    private final OperationInfo info;
    private final EngineContext context;
    private final Consumer<BrokerException> linkLost;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition room = lock.newCondition();
    private final Condition work = lock.newCondition();
    private final Condition drained = lock.newCondition();
    private final ArrayDeque<Entry> queue = new ArrayDeque<>();
    private final LinkedHashSet<Entry> unconfirmed = new LinkedHashSet<>();
    private long bytes;
    private Phase phase = Phase.RUNNING;
    private int epoch;
    private Writer writer;
    private @Nullable BrokerException failure;

    private SendPipeline(
            NodeId node,
            SenderOptions options,
            OperationInfo info,
            EngineContext context,
            Writer writer,
            Consumer<BrokerException> linkLost) {
        this.node = node;
        this.options = options;
        this.info = info;
        this.context = context;
        this.writer = writer;
        this.linkLost = linkLost;
    }

    /**
     * Opens the pipeline and starts its writer thread.
     *
     * @param id the link's name, for thread names
     * @param info the telemetry description of one send on this link
     * @param linkLost called, on the writer thread, when {@code writer} reports the link down; a
     *     driver passes its supervisor's {@code lost}
     * @throws BrokerException {@code INVALID_ARGUMENT} when ordering is asked of an {@code
     *     AT_LEAST_ONCE} sender whose deduplication is below {@code SESSION}: a resend would reorder
     */
    public static SendPipeline open(
            String id,
            NodeId node,
            SenderOptions options,
            OperationInfo info,
            EngineContext context,
            Writer writer,
            Consumer<BrokerException> linkLost) {
        if (options.guarantee() == Guarantee.AT_LEAST_ONCE
                && options.ordering() != OrderingScope.NONE
                && !options.deduplication().satisfies(DeduplicationScope.SESSION)) {
            throw Failures.unorderedResend(node);
        }
        SendPipeline pipeline = new SendPipeline(
                node,
                options,
                info,
                context,
                Objects.requireNonNull(writer, "writer"),
                Objects.requireNonNull(linkLost, "linkLost"));
        context.start("openbroker-writer-" + id, pipeline::run);
        return pipeline;
    }

    /** Waits for room for up to {@code maxBlock}, then for the guarantee's confirmation. */
    public SendReceipt send(OutboundMessage message) {
        Entry entry = prepare(message);
        long remaining = options.buffer().maxBlock().toNanos();
        lock.lock();
        try {
            while (!tryAdd(entry)) {
                if (remaining <= 0) {
                    throw Failures.bufferFull(node);
                }
                remaining = room.awaitNanos(remaining);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            BrokerException cancelled = Failures.cancelled(node, Dispatch.NOT_SENT, e);
            discard(entry, cancelled);
            throw cancelled;
        } catch (BrokerException e) {
            discard(entry, e);
            throw e;
        } finally {
            lock.unlock();
        }
        try {
            return entry.receipt.get();
        } catch (InterruptedException e) {
            throw Deadlines.cancelled(node, e);
        } catch (ExecutionException e) {
            throw (BrokerException) e.getCause();
        }
    }

    /** Takes the message without waiting; {@link Offer.Full} when the window has no room. */
    public Offer offer(OutboundMessage message) {
        Entry entry = prepare(message);
        lock.lock();
        try {
            if (tryAdd(entry)) {
                return new Offer.Accepted(entry.receipt);
            }
            discard(entry, Failures.bufferFull(node));
            return new Offer.Full(queue.size() + unconfirmed.size(), bytes);
        } catch (BrokerException e) {
            discard(entry, e);
            throw e;
        } finally {
            lock.unlock();
        }
    }

    /** Waits until every accepted message is confirmed or failed; {@code DEADLINE_EXCEEDED} when time runs out. */
    public void flush(Duration timeout) {
        long remaining = timeout.toNanos();
        lock.lock();
        try {
            while (!queue.isEmpty() || !unconfirmed.isEmpty()) {
                if (remaining <= 0) {
                    throw Deadlines.exceeded(node, Dispatch.IN_DOUBT);
                }
                remaining = drained.awaitNanos(remaining);
            }
        } catch (InterruptedException e) {
            throw Deadlines.cancelled(node, e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * The link was lost. Writing stops and the window keeps what it holds. {@code AT_MOST_ONCE} and
     * {@code TRANSACTED} messages that were written fail now: {@code UNAVAILABLE}/{@code IN_DOUBT}
     * and {@code ABORTED}/{@code TRANSACTION_ROLLED_BACK}.
     */
    public void suspend(BrokerException cause) {
        List<Entry> doomed = List.of();
        lock.lock();
        try {
            if (phase != Phase.RUNNING && phase != Phase.SUSPENDED) {
                return;
            }
            phase = Phase.SUSPENDED;
            if (options.guarantee() != Guarantee.AT_LEAST_ONCE) {
                doomed = new ArrayList<>(unconfirmed);
                unconfirmed.clear();
                doomed.forEach(this::remove);
            }
        } finally {
            lock.unlock();
        }
        BrokerException reason = options.guarantee() == Guarantee.TRANSACTED
                ? Failures.rolledBack(node)
                : Failures.unavailable(
                        node, Dispatch.IN_DOUBT, "The link was lost before the send was confirmed", cause);
        doomed.forEach(entry -> end(entry, reason));
    }

    /**
     * The link is open again on {@code writer}. Unconfirmed {@code AT_LEAST_ONCE} messages go first,
     * in their original order, marked resent; then writing continues.
     */
    public void resume(Writer writer) {
        lock.lock();
        try {
            if (phase == Phase.FAILED || phase == Phase.CLOSED) {
                return;
            }
            this.writer = Objects.requireNonNull(writer, "writer");
            epoch++;
            List<Entry> again = new ArrayList<>(unconfirmed);
            unconfirmed.clear();
            for (int i = again.size() - 1; i >= 0; i--) {
                again.get(i).resent = true;
                queue.addFirst(again.get(i));
            }
            phase = Phase.RUNNING;
            work.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** The link could not be reopened: every pending receipt fails {@code UNAVAILABLE}, and nothing is written again. */
    public void fail(BrokerException cause) {
        List<Entry> doomed;
        lock.lock();
        try {
            if (phase == Phase.FAILED || phase == Phase.CLOSED) {
                return;
            }
            phase = Phase.FAILED;
            failure = cause;
            doomed = drain();
        } finally {
            lock.unlock();
        }
        doomed.forEach(entry -> end(
                entry,
                Failures.unavailable(
                        node,
                        entry.generation > 0 ? Dispatch.IN_DOUBT : Dispatch.NOT_SENT,
                        "The link could not be recovered",
                        cause)));
    }

    /** Flushes for {@code maxBlock}, then fails the rest {@code CANCELLED}. */
    @Override
    public void close() {
        try {
            flush(options.buffer().maxBlock());
        } catch (BrokerException e) {
            LOG.log(System.Logger.Level.DEBUG, "A sender closed with messages unconfirmed", e);
        }
        List<Entry> doomed;
        lock.lock();
        try {
            if (phase == Phase.CLOSED) {
                return;
            }
            phase = Phase.CLOSED;
            doomed = drain();
        } finally {
            lock.unlock();
        }
        doomed.forEach(entry -> end(
                entry, Failures.cancelled(node, entry.generation > 0 ? Dispatch.IN_DOUBT : Dispatch.NOT_SENT, null)));
    }

    int inFlight() {
        lock.lock();
        try {
            return queue.size() + unconfirmed.size();
        } finally {
            lock.unlock();
        }
    }

    private Entry prepare(OutboundMessage message) {
        OutboundMessage prepared = message;
        if (options.deduplication() != DeduplicationScope.NONE
                && message.headers().deduplicationId().isEmpty()) {
            prepared = message.withHeaders(message.headers().toBuilder()
                    .deduplicationId(uuidV7(context.clock(), context.random()))
                    .build());
        }
        MessagingObserver observer = context.observer();
        Observation observation = null;
        if (observer != MessagingObserver.none()) {
            Map<String, PropertyValue> carried = new LinkedHashMap<>();
            observation = observer.onSend(info, prepared, (key, value) -> carried.put(key, PropertyValue.of(value)));
            if (!carried.isEmpty()) {
                prepared = prepared.withProperties(carried);
            }
        }
        return new Entry(prepared, prepared.body().size(), observation);
    }

    /** Adds the entry when the window has room; the caller holds the lock. */
    private boolean tryAdd(Entry entry) {
        if (phase == Phase.CLOSED) {
            throw Failures.closed(node);
        }
        if (phase == Phase.FAILED) {
            throw Failures.unavailable(node, Dispatch.NOT_SENT, "The link failed", failure);
        }
        int count = queue.size() + unconfirmed.size();
        boolean empty = count == 0;
        if (!empty
                && (count >= options.buffer().maxInFlight()
                        || bytes + entry.size > options.buffer().maxBytes())) {
            return false;
        }
        queue.addLast(entry);
        bytes += entry.size;
        work.signalAll();
        return true;
    }

    /** Takes every pending entry out; the caller holds the lock. */
    private List<Entry> drain() {
        List<Entry> all = new ArrayList<>(unconfirmed);
        all.addAll(queue);
        unconfirmed.clear();
        queue.clear();
        bytes = 0;
        room.signalAll();
        work.signalAll();
        drained.signalAll();
        return all;
    }

    /** Gives the entry's room back; the caller holds the lock and has taken it out of the sets. */
    private void remove(Entry entry) {
        bytes -= entry.size;
        room.signalAll();
        if (queue.isEmpty() && unconfirmed.isEmpty()) {
            drained.signalAll();
        }
    }

    /** Completes the entry's receipt on a virtual thread. */
    private void end(Entry entry, @Nullable BrokerException failure, @Nullable SendReceipt receipt) {
        context.completions().execute(() -> {
            Observation observation = entry.observation;
            if (observation != null) {
                try {
                    observation.end(failure);
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.WARNING, "An observer failed", e);
                }
            }
            if (failure != null) {
                entry.receipt.completeExceptionally(failure);
            } else {
                entry.receipt.complete(receipt);
            }
        });
    }

    /** Ends an entry that was never accepted: nobody holds its receipt. */
    private void discard(Entry entry, BrokerException failure) {
        Observation observation = entry.observation;
        if (observation != null) {
            try {
                observation.end(failure);
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "An observer failed", e);
            }
        }
    }

    private void end(Entry entry, BrokerException failure) {
        end(entry, failure, null);
    }

    private void run() {
        while (true) {
            Entry entry;
            Writer current;
            int generation;
            int seen;
            lock.lock();
            try {
                while ((phase == Phase.SUSPENDED || queue.isEmpty())
                        && phase != Phase.CLOSED
                        && phase != Phase.FAILED) {
                    work.awaitUninterruptibly();
                }
                if (phase == Phase.CLOSED || phase == Phase.FAILED) {
                    return;
                }
                entry = queue.pollFirst();
                generation = ++entry.generation;
                unconfirmed.add(entry);
                current = writer;
                seen = epoch;
            } finally {
                lock.unlock();
            }
            write(current, entry, generation, seen);
        }
    }

    private void write(Writer current, Entry entry, int generation, int seen) {
        try {
            current.write(entry.message, new Ack(entry, generation));
        } catch (BrokerException e) {
            if (e.status() == Status.UNAVAILABLE && e.dispatch() == Dispatch.NOT_SENT) {
                if (requeue(entry, generation, seen)) {
                    linkLost.accept(e);
                }
            } else {
                settle(entry, generation, e, null);
            }
        } catch (RuntimeException e) {
            settle(entry, generation, Failures.unexpected(node, e), null);
        }
    }

    /** Puts a message the writer could not write back at the head and waits for the recovery. */
    private boolean requeue(Entry entry, int generation, int seen) {
        lock.lock();
        try {
            if (entry.generation != generation || !unconfirmed.remove(entry)) {
                return false;
            }
            entry.generation--;
            queue.addFirst(entry);
            if (phase == Phase.RUNNING && epoch == seen) {
                phase = Phase.SUSPENDED;
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    private void settle(Entry entry, int generation, @Nullable BrokerException failure, @Nullable SendReceipt receipt) {
        lock.lock();
        try {
            if (entry.generation != generation || !unconfirmed.remove(entry)) {
                return;
            }
            remove(entry);
        } finally {
            lock.unlock();
        }
        end(entry, failure, receipt);
    }

    private final class Ack implements Confirmation {

        private final Entry entry;
        private final int generation;

        Ack(Entry entry, int generation) {
            this.entry = entry;
            this.generation = generation;
        }

        @Override
        public void confirmed(SendReceipt receipt) {
            settle(
                    entry,
                    generation,
                    null,
                    new SendReceipt(
                            receipt.messageId(), receipt.ref(), receipt.node(), receipt.resent() || entry.resent));
        }

        @Override
        public void failed(BrokerException failure) {
            settle(entry, generation, failure, null);
        }
    }

    /** A time-ordered UUID (version 7): the millisecond clock, then random bits. */
    static String uuidV7(Clock clock, RandomGenerator random) {
        long high = (clock.millis() << 16) | 0x7000L | (random.nextLong() & 0x0FFFL);
        long low = (random.nextLong() & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;
        return new UUID(high, low).toString();
    }
}
