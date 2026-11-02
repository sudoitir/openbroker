package io.github.sudoitir.openbroker.rabbitmq.messaging;

import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.FlowControl;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.support.messaging.ProcessorEngine;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The {@link ProcessorEngine.Lanes} over {@code RabbitMqReceiver}s: each lane holds at most one unsettled message.
 *
 * <p>A lane is a consumer of its own with a prefetch of one, so the queue hands a message to whichever lane is free.
 * RabbitMQ keeps no group on one lane, so {@link #pinsGroups()} is false. A rejected message goes to the policy's
 * dead-letter target through the {@link DeadLetterSink} when it names one, and otherwise is rejected without requeue,
 * so the queue's own dead-letter exchange applies.
 */
final class RabbitMqLanes implements ProcessorEngine.Lanes, AutoCloseable {

    private static final String REJECTED = "rejected";

    private final LinkEnv env;
    private final String processorId;
    private final ResourceId subscription;
    private final ProcessorOptions options;
    private final DeadLetterSink deadLetters;
    private final List<Lane> lanes = new CopyOnWriteArrayList<>();

    /** Lanes over connections of their own, for a caller that has no transport. */
    RabbitMqLanes(
            RabbitMqCluster cluster,
            String processorId,
            ResourceId subscription,
            ProcessorOptions options,
            DeadLetterSink deadLetters) {
        this(new LinkEnv(cluster), processorId, subscription, options, deadLetters);
    }

    RabbitMqLanes(
            LinkEnv env,
            String processorId,
            ResourceId subscription,
            ProcessorOptions options,
            DeadLetterSink deadLetters) {
        this.env = env;
        this.processorId = processorId;
        this.subscription = subscription;
        this.options = options;
        this.deadLetters = deadLetters;
    }

    String processorId() {
        return processorId;
    }

    @Override
    public ProcessorEngine.Lane open(int index) {
        Lane lane = new Lane(
                new RabbitMqReceiver(env, subscription, options.receive().withFlow(FlowControl.none())));
        lanes.add(lane);
        return lane;
    }

    @Override
    public boolean pinsGroups() {
        return false;
    }

    @Override
    public LinkState state() {
        boolean recovering = false;
        boolean open = false;
        for (Lane lane : lanes) {
            switch (lane.receiver.state()) {
                case FAILED -> {
                    return LinkState.FAILED;
                }
                case RECOVERING -> recovering = true;
                case OPEN -> open = true;
                case CLOSED -> {
                    // a closed lane says nothing about the others
                }
            }
        }
        if (recovering) {
            return LinkState.RECOVERING;
        }
        return open || lanes.isEmpty() ? LinkState.OPEN : LinkState.CLOSED;
    }

    @Override
    public Sender deadLetter(SendTarget target) {
        return deadLetters.sender(target);
    }

    @Override
    public void close() {
        lanes.forEach(Lane::close);
    }

    private final class Lane implements ProcessorEngine.Lane {

        private final RabbitMqReceiver receiver;

        Lane(RabbitMqReceiver receiver) {
            this.receiver = receiver;
        }

        @Override
        public Optional<Message> next(Duration timeout) throws InterruptedException {
            List<Message> next = receiver.receiveInterruptibly(1, timeout);
            return next.isEmpty() ? Optional.empty() : Optional.of(next.getFirst());
        }

        @Override
        public void settle(Message message, Disposition disposition) {
            if (disposition instanceof Disposition.Reject reject
                    && options.retry().deadLetter().isPresent()) {
                deadLetter(message, reject);
                return;
            }
            receiver.settle(message, disposition);
        }

        /** Sends the message to the policy's target and only then acknowledges it; a send that fails requeues it. */
        private void deadLetter(Message message, Disposition.Reject reject) {
            try {
                deadLetters.reject(
                        message,
                        message.headers().deliveryCount().orElse(1),
                        reject.reason().orElse(REJECTED));
            } catch (RuntimeException e) {
                receiver.settle(message, new Disposition.Release(Optional.empty(), true));
                throw e;
            }
            receiver.settle(message, Disposition.accept());
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
