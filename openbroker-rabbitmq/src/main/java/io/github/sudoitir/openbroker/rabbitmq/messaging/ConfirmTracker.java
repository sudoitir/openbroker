package io.github.sudoitir.openbroker.rabbitmq.messaging;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import org.jspecify.annotations.Nullable;

/**
 * Tracks publisher confirms of one channel: a publish sequence number to its pending receipt. An ack (possibly
 * {@code multiple}) settles it, a nack fails it {@code DESTINATION_FULL}, a return before the ack fails it {@code
 * NOT_ROUTED}, and a lost channel completes the unconfirmed with an in-doubt failure that {@link #isLoss} recognises,
 * for the sender to leave to the recovery, which writes them again.
 *
 * <p>A {@code basic.return} names the message by its exchange, routing key and message id and not by its sequence
 * number, so a publish is registered with such a key and a return finds the oldest pending publish under it.
 */
final class ConfirmTracker {

    /** The failure the unconfirmed publishes of a lost channel complete with. */
    private static final class Loss extends BrokerException {

        private static final long serialVersionUID = 1L;

        Loss() {
            super(
                    Status.UNAVAILABLE,
                    ErrorInfo.of(ErrorReasons.UNREACHABLE),
                    Dispatch.IN_DOUBT,
                    RabbitMqCluster.CLUSTER,
                    "The channel was lost before the publish was confirmed");
        }
    }

    private record Pending(
            CompletableFuture<SendReceipt> future,
            SendReceipt receipt,
            String key,
            @Nullable BrokerException returned) {

        Pending returnedWith(BrokerException failure) {
            return new Pending(future, receipt, key, failure);
        }
    }

    private final Optional<ResourceId> destination;
    private final TreeMap<Long, Pending> pending = new TreeMap<>();

    /** A tracker for publishes to {@code destination}, which the failures name. */
    ConfirmTracker(Optional<ResourceId> destination) {
        this.destination = destination;
    }

    ConfirmTracker() {
        this(Optional.empty());
    }

    /** Whether {@code failure} is what {@link #channelLost} completes the unconfirmed with. */
    static boolean isLoss(Throwable failure) {
        return failure instanceof Loss;
    }

    /** Registers a publish that completes {@code future} with the plain receipt. */
    void published(long sequence, CompletableFuture<SendReceipt> future) {
        published(sequence, "", future, SendReceipt.accepted());
    }

    /** Registers the publish with {@code sequence}, which a return finds again by {@code key}. */
    synchronized void published(long sequence, String key, CompletableFuture<SendReceipt> future, SendReceipt receipt) {
        pending.put(sequence, new Pending(future, receipt, key, null));
    }

    /** Forgets a publish that was never written, and returns whether it was still waiting. */
    synchronized boolean withdraw(long sequence) {
        return pending.remove(sequence) != null;
    }

    /** The broker confirmed {@code sequence}, and every earlier one when {@code multiple}. */
    void ack(long sequence, boolean multiple) {
        for (Pending done : take(sequence, multiple)) {
            BrokerException returned = done.returned();
            if (returned != null) {
                done.future().completeExceptionally(returned);
            } else {
                done.future().complete(done.receipt());
            }
        }
    }

    /** The broker refused {@code sequence}, and every earlier one when {@code multiple}. */
    void nack(long sequence, boolean multiple) {
        for (Pending done : take(sequence, multiple)) {
            BrokerException returned = done.returned();
            done.future().completeExceptionally(returned != null ? returned : RabbitMqErrors.nacked(destination));
        }
    }

    /** The broker returned the publish with {@code sequence}: it routed to no queue. */
    synchronized void returned(long sequence, int replyCode, String replyText) {
        Pending found = pending.get(sequence);
        if (found != null) {
            pending.put(sequence, found.returnedWith(RabbitMqErrors.fromReply(replyCode, replyText, destination)));
        }
    }

    /**
     * The broker returned a publish registered under {@code key}: the oldest pending one is the one.
     *
     * @return whether a pending publish was found
     */
    synchronized boolean returned(String key, int replyCode, String replyText) {
        for (Map.Entry<Long, Pending> entry : pending.entrySet()) {
            if (entry.getValue().returned() == null && entry.getValue().key().equals(key)) {
                returned(entry.getKey(), replyCode, replyText);
                return true;
            }
        }
        return false;
    }

    /** The channel was lost: every unconfirmed publish is in doubt. */
    void channelLost() {
        List<Pending> lost;
        synchronized (this) {
            lost = new ArrayList<>(pending.values());
            pending.clear();
        }
        Loss loss = new Loss();
        for (Pending one : lost) {
            one.future().completeExceptionally(loss);
        }
    }

    /** How many publishes wait for their confirm. */
    synchronized int unconfirmed() {
        return pending.size();
    }

    private synchronized List<Pending> take(long sequence, boolean multiple) {
        List<Pending> taken = new ArrayList<>();
        if (multiple) {
            Iterator<Pending> it = pending.headMap(sequence, true).values().iterator();
            while (it.hasNext()) {
                taken.add(it.next());
                it.remove();
            }
        } else {
            Pending one = pending.remove(sequence);
            if (one != null) {
                taken.add(one);
            }
        }
        return taken;
    }
}
