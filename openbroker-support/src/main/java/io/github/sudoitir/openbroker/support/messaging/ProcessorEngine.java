package io.github.sudoitir.openbroker.support.messaging;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageContext;
import io.github.sudoitir.openbroker.messaging.MessageHandler;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import io.github.sudoitir.openbroker.messaging.Observation;
import io.github.sudoitir.openbroker.messaging.OperationInfo;
import io.github.sudoitir.openbroker.messaging.OrderingScope;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.Processor;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.github.sudoitir.openbroker.messaging.RetryMode;
import io.github.sudoitir.openbroker.messaging.RetryPolicy;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.ReentrantLock;
import org.jspecify.annotations.Nullable;

/**
 * Push processing over lanes. A lane is a receiver holding at most one unsettled message; the engine
 * runs one virtual thread per lane ({@link ProcessorOptions#concurrency()} lanes), calls the handler
 * on it and settles the message from the handler's {@link Disposition}.
 *
 * <ul>
 *   <li>{@code NONE}: any lane takes any message.
 *   <li>{@code GROUP} and {@code PARTITION}: messages that share a group id (or key, or shard) run
 *       one at a time. When {@link Lanes#pinsGroups()} the system already keeps a group on one lane
 *       and the engine does nothing; otherwise it hashes the group onto one of {@code concurrency}
 *       fair locks, so lanes that hold messages of one group take turns in arrival order.
 *   <li>{@code SUBSCRIPTION}: one lane, so total order.
 * </ul>
 *
 * <p>A handler that throws, or returns a release that counts as a failed delivery, is retried by the
 * {@link RetryPolicy}: {@code SYSTEM} releases it counted and leaves the rest to the system; {@code
 * MANAGED} waits out the backoff and calls the handler again in place when ordering is on, or
 * releases it with that delay when it is not, and after {@code maxAttempts} dead-letters it.
 */
public final class ProcessorEngine implements Processor {

    private static final System.Logger LOG = System.getLogger(ProcessorEngine.class.getName());
    private static final Duration POLL = Duration.ofSeconds(1);
    private static final Duration FORCE_WAIT = Duration.ofSeconds(5);

    /** One receiver of the driver, holding at most one unsettled message. Used by one thread. */
    public interface Lane extends AutoCloseable {

        /**
         * The next message, waiting up to {@code timeout}; empty on timeout and while the link
         * recovers. Throws {@link InterruptedException} when the engine stops; throw a {@link
         * BrokerException} for a failure the engine should back off from and retry.
         */
        Optional<Message> next(Duration timeout) throws InterruptedException;

        /** Settles the message this lane delivered. {@code DELIVERY_STALE} when the link recovered since. */
        void settle(Message message, Disposition disposition);

        /** A sender joined to the transaction of the delivery in hand; for {@code TRANSACTED} lanes. */
        Sender sender(SendTarget target);

        @Override
        void close();
    }

    /** What a driver gives the engine. */
    public interface Lanes {

        /** Opens lane {@code index}, from 0 to {@code concurrency - 1}. */
        Lane open(int index);

        /** Whether the system keeps messages of one group on one lane, as a group-aware broker does. */
        boolean pinsGroups();

        /** The state of the underlying link. */
        LinkState state();

        /** A sender to the dead-letter target of the retry policy. Called once, only when a target is set. */
        Sender deadLetter(SendTarget target);
    }

    private static final int IDLE = 0;
    private static final int BUSY = 1;
    private static final int STOPPED = 2;

    private final String id;
    private final ProcessorOptions options;
    private final OperationInfo info;
    private final EngineContext context;
    private final MessageHandler handler;
    private final Lanes lanes;
    private final @Nullable Sender deadLetter;
    private final ReentrantLock @Nullable [] groups;
    private final AtomicIntegerArray phases;
    private final AtomicBoolean stopping = new AtomicBoolean();
    private final Thread[] threads;
    private volatile boolean forced;
    private volatile boolean closed;

    private ProcessorEngine(
            String id,
            ProcessorOptions options,
            OperationInfo info,
            EngineContext context,
            MessageHandler handler,
            Lanes lanes,
            @Nullable Sender deadLetter) {
        this.id = id;
        this.options = options;
        this.info = info;
        this.context = context;
        this.handler = handler;
        this.lanes = lanes;
        this.deadLetter = deadLetter;
        int n = options.concurrency();
        boolean hashed = n > 1 && !lanes.pinsGroups() && grouped(options.ordering());
        if (hashed) {
            groups = new ReentrantLock[n];
            for (int i = 0; i < n; i++) {
                groups[i] = new ReentrantLock(true);
            }
        } else {
            groups = null;
        }
        this.phases = new AtomicIntegerArray(n);
        this.threads = new Thread[n];
    }

    /**
     * Opens the lanes and starts one virtual thread on each.
     *
     * @param id the processor's name
     * @param options the options, whose retry policy is resolved (never {@code DEFAULT})
     * @param info the telemetry description of one handler call
     */
    public static ProcessorEngine start(
            String id,
            ProcessorOptions options,
            OperationInfo info,
            EngineContext context,
            Lanes lanes,
            MessageHandler handler) {
        RetryPolicy retry = options.retry();
        if (retry.mode() == RetryMode.DEFAULT) {
            throw new IllegalArgumentException("Resolve RetryMode.DEFAULT with Retries.resolve before starting");
        }
        Sender deadLetter =
                retry.mode() == RetryMode.MANAGED && retry.deadLetter().isPresent()
                        ? lanes.deadLetter(retry.deadLetter().get())
                        : null;
        ProcessorEngine engine = new ProcessorEngine(id, options, info, context, handler, lanes, deadLetter);
        List<Lane> opened = new ArrayList<>();
        try {
            for (int i = 0; i < options.concurrency(); i++) {
                opened.add(lanes.open(i));
            }
        } catch (RuntimeException e) {
            opened.forEach(ProcessorEngine::closeQuietly);
            if (deadLetter != null) {
                closeQuietly(deadLetter);
            }
            throw e;
        }
        for (int i = 0; i < opened.size(); i++) {
            Lane lane = opened.get(i);
            int index = i;
            engine.threads[i] = context.start("openbroker-lane-" + id + "-" + i, () -> engine.run(index, lane));
        }
        return engine;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public LinkState state() {
        return closed ? LinkState.CLOSED : lanes.state();
    }

    @Override
    public RetryPolicy retry() {
        return options.retry();
    }

    /** Stops taking messages, waits up to {@code grace} for the handlers, then interrupts them; what is left is released uncounted. */
    @Override
    public void stop(Duration grace) {
        if (stopping.compareAndSet(false, true)) {
            for (int i = 0; i < threads.length; i++) {
                if (phases.compareAndSet(i, IDLE, STOPPED)) {
                    threads[i].interrupt();
                }
            }
        }
        long deadline = System.nanoTime() + grace.toNanos();
        awaitAll(Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
        if (anyAlive()) {
            forced = true;
            for (Thread thread : threads) {
                thread.interrupt();
            }
            awaitAll(FORCE_WAIT);
        }
        if (!closed) {
            closed = true;
            if (deadLetter != null) {
                closeQuietly(deadLetter);
            }
        }
    }

    /** {@link #stop} with the options' {@code stopGrace}. */
    @Override
    public void close() {
        stop(options.stopGrace());
    }

    private boolean anyAlive() {
        for (Thread thread : threads) {
            if (thread.isAlive()) {
                return true;
            }
        }
        return false;
    }

    private void awaitAll(Duration limit) {
        long deadline = System.nanoTime() + limit.toNanos();
        for (Thread thread : threads) {
            try {
                thread.join(Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void run(int index, Lane lane) {
        int failures = 0;
        try {
            while (!stopping.get()) {
                Optional<Message> next;
                try {
                    next = lane.next(POLL);
                    failures = 0;
                } catch (InterruptedException e) {
                    return;
                } catch (BrokerException e) {
                    LOG.log(System.Logger.Level.DEBUG, "A lane could not receive; backing off", e);
                    try {
                        context.sleeper()
                                .sleep(Backoffs.delay(
                                        options.receive().recovery().backoff(), ++failures, context.random()));
                    } catch (InterruptedException interrupted) {
                        return;
                    }
                    continue;
                }
                if (next.isEmpty()) {
                    continue;
                }
                if (!phases.compareAndSet(index, IDLE, BUSY)) {
                    settle(lane, next.get(), new Disposition.Release(Optional.empty(), false));
                    return;
                }
                try {
                    process(lane, next.get());
                } finally {
                    phases.compareAndSet(index, BUSY, IDLE);
                }
            }
        } finally {
            Thread.interrupted();
            closeQuietly(lane);
        }
    }

    private void process(Lane lane, Message message) {
        ReentrantLock group = groupOf(message);
        if (group != null) {
            try {
                group.lockInterruptibly();
            } catch (InterruptedException e) {
                settle(lane, message, new Disposition.Release(Optional.empty(), false));
                return;
            }
        }
        try {
            settle(lane, message, decide(lane, message));
        } finally {
            if (group != null) {
                group.unlock();
            }
        }
    }

    private @Nullable ReentrantLock groupOf(Message message) {
        if (groups == null) {
            return null;
        }
        int hash;
        if (options.ordering() == OrderingScope.PARTITION) {
            hash = message.ref().shard().hashCode();
        } else if (message.headers().groupId().isPresent()) {
            hash = message.headers().groupId().get().hashCode();
        } else if (message.headers().key().isPresent()) {
            hash = message.headers().key().get().hashCode();
        } else {
            return null;
        }
        return groups[Math.floorMod(hash, groups.length)];
    }

    /** Calls the handler, retrying per policy, until there is a disposition to settle with. */
    private Disposition decide(Lane lane, Message message) {
        RetryPolicy retry = options.retry();
        boolean inPlace = options.ordering() != OrderingScope.NONE;
        int attempt = message.headers().deliveryCount().orElse(1);
        while (true) {
            Throwable error = null;
            Disposition result = null;
            Observation observation = observe(message);
            try (AutoCloseable scope = scope(observation)) {
                result = handler.handle(new Delivery(message, attempt, lane));
            } catch (InterruptedException e) {
                return interrupted(observation, e);
            } catch (Exception e) {
                error = e;
            }
            if (observation != null) {
                end(observation, error);
            }
            if (forced && Thread.currentThread().isInterrupted()) {
                return new Disposition.Release(Optional.empty(), false);
            }
            boolean failed = result == null || result instanceof Disposition.Release r && r.deliveryFailed();
            if (!failed) {
                return result;
            }
            if (retry.mode() == RetryMode.SYSTEM) {
                return result != null ? result : new Disposition.Release(Optional.empty(), true);
            }
            if (attempt >= retry.maxAttempts()) {
                return deadLetter(message, attempt, error);
            }
            Duration delay = Backoffs.delay(retry.backoff(), attempt, context.random());
            if (!inPlace) {
                return new Disposition.Release(Optional.of(delay), true);
            }
            try {
                context.sleeper().sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new Disposition.Release(Optional.empty(), false);
            }
            attempt++;
        }
    }

    private Disposition interrupted(@Nullable Observation observation, InterruptedException e) {
        Thread.currentThread().interrupt();
        if (observation != null) {
            end(observation, e);
        }
        return new Disposition.Release(Optional.empty(), false);
    }

    private @Nullable Observation observe(Message message) {
        MessagingObserver observer = context.observer();
        return observer == MessagingObserver.none() ? null : observer.onProcess(info, message);
    }

    private static AutoCloseable scope(@Nullable Observation observation) {
        return observation == null ? () -> {} : observation.scope();
    }

    private static void end(Observation observation, @Nullable Throwable error) {
        try {
            observation.end(error);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "An observer failed", e);
        }
    }

    /** Rejects to the policy's dead-letter target, or to the system's own dead-letter destination. */
    private Disposition deadLetter(Message message, int attempt, @Nullable Throwable error) {
        if (deadLetter == null) {
            return new Disposition.Reject(Optional.of("The handler failed " + attempt + " times"));
        }
        OutboundMessage copy = OutboundMessage.copyOf(message)
                .withHeaders(message.headers().toBuilder().deliveryCount(null).build())
                .withProperties(Map.of(
                        "openbroker.dead_letter_attempts",
                        PropertyValue.of(attempt),
                        "openbroker.dead_letter_reason",
                        PropertyValue.of(
                                error == null
                                        ? ErrorReasons.UNEXPECTED_SYSTEM
                                        : error.getClass().getSimpleName())));
        try {
            deadLetter.send(copy);
            return Disposition.accept();
        } catch (BrokerException e) {
            LOG.log(System.Logger.Level.WARNING, "A message could not be dead-lettered", e);
            return new Disposition.Release(Optional.empty(), true);
        }
    }

    private static void settle(Lane lane, Message message, Disposition disposition) {
        try {
            lane.settle(message, disposition);
        } catch (BrokerException e) {
            boolean stale = ErrorReasons.DELIVERY_STALE.equals(e.reason());
            LOG.log(
                    stale ? System.Logger.Level.DEBUG : System.Logger.Level.WARNING,
                    "A message could not be settled",
                    e);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "A message could not be settled", e);
        }
    }

    private static boolean grouped(OrderingScope ordering) {
        return ordering == OrderingScope.GROUP || ordering == OrderingScope.PARTITION;
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            LOG.log(System.Logger.Level.DEBUG, "Closing failed", e);
        }
    }

    private record Delivery(Message message, int attempt, Lane lane) implements MessageContext {

        @Override
        public Sender sender(SendTarget target) {
            return lane.sender(target);
        }
    }
}
