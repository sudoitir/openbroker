package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.FlowControl;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.support.messaging.ProcessorEngine;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.jspecify.annotations.Nullable;

/**
 * The lanes of one processor: receivers of their own, each holding one message.
 *
 * <p>A lane's consumer reads ahead of nothing. A consumer that buffers takes the messages the other
 * lanes, and the other processors of the queue, would have run now; a lane that holds one message at
 * a time asks the broker for the next when it is free. Artemis keeps the messages of one group on
 * one consumer, so {@link #pinsGroups()} is true and the engine adds no locking of its own.
 *
 * <p>A {@code TRANSACTED} lane commits when its message is accepted, with whatever the handler sent
 * through {@link ProcessorEngine.Lane#sender}, and rolls back when it is released. A rejection rolls
 * back counted, so the broker's {@code max-delivery-attempts} dead-letters it.
 */
final class CoreLanes implements ProcessorEngine.Lanes {

    private final TransportEnv env;
    private final String processorId;
    private final NodeHandle node;
    private final ResourceId subscription;
    private final String destination;
    private final String selector;
    private final ProcessorOptions options;
    private final Function<SendTarget, Sender> senders;
    private final List<CoreReceiver> receivers = new CopyOnWriteArrayList<>();
    private final ReentrantLock deadLetterLock = new ReentrantLock();
    private @Nullable Sender deadLetter;

    CoreLanes(
            TransportEnv env,
            String processorId,
            NodeHandle node,
            ResourceId subscription,
            String destination,
            String selector,
            ProcessorOptions options,
            Function<SendTarget, Sender> senders) {
        this.env = env;
        this.processorId = processorId;
        this.node = node;
        this.subscription = subscription;
        this.destination = destination;
        this.selector = selector;
        this.options = options;
        this.senders = senders;
    }

    @Override
    public ProcessorEngine.Lane open(int index) {
        ReceiverOptions lane = new ReceiverOptions(
                options.receive().mode(),
                options.receive().guarantee(),
                options.receive().filter(),
                options.receive().producedAtOrBefore(),
                new FlowControl(1, 0, 0),
                options.receive().recovery());
        try {
            CoreReceiver receiver = CoreReceiver.open(
                    env,
                    processorId + "-" + index,
                    node,
                    subscription,
                    destination,
                    selector,
                    lane,
                    0,
                    this::policyDeadLetter);
            receivers.add(receiver);
            return new Lane(receiver, options.receive().guarantee() == Guarantee.TRANSACTED);
        } catch (ActiveMQException e) {
            throw CoreLinks.failure(node.id(), e, Optional.of(subscription));
        }
    }

    @Override
    public boolean pinsGroups() {
        return true;
    }

    @Override
    public LinkState state() {
        LinkState state = LinkState.OPEN;
        for (CoreReceiver receiver : receivers) {
            LinkState lane = receiver.state();
            if (lane == LinkState.FAILED) {
                return LinkState.FAILED;
            }
            if (lane == LinkState.RECOVERING) {
                state = LinkState.RECOVERING;
            }
        }
        return state;
    }

    @Override
    public Sender deadLetter(SendTarget target) {
        return deadLetterSender(target);
    }

    /** The sender to the policy's dead-letter target, opened on first use and shared by the lanes. */
    private Sender deadLetterSender(SendTarget target) {
        deadLetterLock.lock();
        try {
            if (deadLetter == null) {
                deadLetter = senders.apply(target);
            }
            return deadLetter;
        } finally {
            deadLetterLock.unlock();
        }
    }

    private Optional<Sender> policyDeadLetter() {
        return options.retry().deadLetter().map(this::deadLetterSender);
    }

    /** Closes what the lanes share; the lanes close themselves. */
    void close() {
        deadLetterLock.lock();
        try {
            if (deadLetter != null) {
                deadLetter.close();
                deadLetter = null;
            }
        } finally {
            deadLetterLock.unlock();
        }
    }

    private static final class Lane implements ProcessorEngine.Lane {

        private final CoreReceiver receiver;
        private final boolean transacted;

        Lane(CoreReceiver receiver, boolean transacted) {
            this.receiver = receiver;
            this.transacted = transacted;
        }

        @Override
        public Optional<Message> next(Duration timeout) throws InterruptedException {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            try {
                List<Message> received = receiver.receive(1, timeout);
                return received.isEmpty() ? Optional.empty() : Optional.of(received.getFirst());
            } catch (BrokerException e) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                throw e;
            }
        }

        @Override
        public void settle(Message message, Disposition disposition) {
            if (!transacted) {
                receiver.settle(message, disposition);
                return;
            }
            switch (disposition) {
                case Disposition.Accept a -> {
                    receiver.settle(message, a);
                    receiver.commit();
                }
                case Disposition.Release r -> receiver.rollback(r.deliveryFailed());
                case Disposition.Reject r -> receiver.rollback(true);
            }
        }

        @Override
        public Sender sender(SendTarget target) {
            return receiver.sender(target);
        }

        @Override
        public void close() {
            receiver.close();
        }
    }
}
