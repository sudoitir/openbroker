package io.github.sudoitir.openbroker.tck;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.Offer;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.github.sudoitir.openbroker.messaging.Receiver;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

/** Bounded waits and message helpers shared by the messaging and chaos cases. */
final class TckSupport {

    /** The property that carries a message's sequence number. */
    static final String SEQ = "seq";

    private static final Duration POLL = Duration.ofMillis(25);

    private TckSupport() {}

    /** The outcome of one send: its receipt, or the reason it failed. */
    record Result(Optional<SendReceipt> receipt, Optional<Throwable> error) {

        BrokerException failure() {
            return (BrokerException) error.orElseThrow();
        }
    }

    // Waiting.

    /** Polls {@code condition} until it holds, failing with {@code what} after {@code timeout}. */
    static void await(String what, Duration timeout, BooleanSupplier condition) {
        long until = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > until) {
                throw new AssertionError("Timed out after " + timeout + " waiting for " + what);
            }
            pause(POLL);
        }
    }

    static void pause(Duration time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted", e);
        }
    }

    // Messages.

    static OutboundMessage message(long seq) {
        return message(seq, MessageHeaders.empty());
    }

    static OutboundMessage message(long seq, MessageHeaders headers) {
        return new OutboundMessage(
                headers,
                Map.of(SEQ, PropertyValue.of(seq)),
                MessageBody.of(("tck-" + seq).getBytes(StandardCharsets.UTF_8)),
                Map.of());
    }

    static long seq(Message message) {
        PropertyValue value = message.properties().get(SEQ);
        if (value == null) {
            throw new AssertionError("A message without a " + SEQ + " property: " + message.ref());
        }
        return Long.parseLong(value.asText());
    }

    static List<Long> seqs(List<Message> messages) {
        return messages.stream().map(TckSupport::seq).toList();
    }

    // Sending.

    /**
     * Offers every message in order, waiting (bounded) whenever the sender is full, and returns the
     * receipts in the same order.
     */
    static List<CompletionStage<SendReceipt>> offerAll(
            Sender sender, List<OutboundMessage> messages, Duration timeout) {
        long until = System.nanoTime() + timeout.toNanos();
        List<CompletionStage<SendReceipt>> receipts = new ArrayList<>();
        for (OutboundMessage message : messages) {
            while (true) {
                if (sender.offer(message) instanceof Offer.Accepted accepted) {
                    receipts.add(accepted.receipt());
                    break;
                }
                if (System.nanoTime() > until) {
                    throw new AssertionError("The sender stayed full for " + timeout);
                }
                pause(Duration.ofMillis(5));
            }
        }
        return receipts;
    }

    /** Waits for every receipt, whether it succeeds or fails. */
    static List<Result> results(List<CompletionStage<SendReceipt>> receipts, Duration timeout) {
        long until = System.nanoTime() + timeout.toNanos();
        List<Result> results = new ArrayList<>();
        for (CompletionStage<SendReceipt> receipt : receipts) {
            long left = Math.max(1, until - System.nanoTime());
            try {
                results.add(new Result(
                        Optional.of(receipt.toCompletableFuture().get(left, TimeUnit.NANOSECONDS)), Optional.empty()));
            } catch (ExecutionException e) {
                results.add(new Result(Optional.empty(), Optional.of(e.getCause())));
            } catch (TimeoutException e) {
                throw new AssertionError("A receipt was still pending after " + timeout, e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted", e);
            }
        }
        return results;
    }

    /** Waits for every receipt and requires each to have succeeded. */
    static List<SendReceipt> confirm(List<CompletionStage<SendReceipt>> receipts, Duration timeout) {
        List<SendReceipt> confirmed = new ArrayList<>();
        for (Result result : results(receipts, timeout)) {
            if (result.error().isPresent()) {
                throw new AssertionError(
                        "A send failed: " + result.error().get(), result.error().get());
            }
            confirmed.add(result.receipt().orElseThrow());
        }
        return confirmed;
    }

    /** Sends, treating a refused duplicate as sent: the destination holds the message either way. */
    static void sendTolerating(Sender sender, OutboundMessage message) {
        try {
            sender.send(message);
        } catch (BrokerException e) {
            if (!ErrorReasons.DUPLICATE_REJECTED.equals(e.reason())) {
                throw e;
            }
        }
    }

    // Receiving.

    /** Receives up to {@code count} messages, waiting at most {@code timeout} in all. */
    static List<Message> receiveN(Receiver receiver, int count, Duration timeout) {
        long until = System.nanoTime() + timeout.toNanos();
        List<Message> got = new ArrayList<>();
        while (got.size() < count && System.nanoTime() < until) {
            got.addAll(receiver.receive(count - got.size(), Duration.ofMillis(500)));
        }
        return got;
    }

    /**
     * Consumes and accepts what the subscription holds until a poll of {@code quiet} finds nothing,
     * and returns it. Messages are never left unsettled.
     */
    static List<Message> drain(Transport transport, ResourceId subscription, Duration quiet) {
        return drain(transport, subscription, 0, Duration.ZERO, quiet);
    }

    /**
     * Like {@link #drain(Transport, ResourceId, Duration)}, but keeps trying (through failures to
     * open or read, such as a broker that is still starting) until it holds {@code minimum} messages
     * or {@code timeout} has passed, and only then ends on a quiet poll.
     */
    static List<Message> drain(
            Transport transport, ResourceId subscription, int minimum, Duration timeout, Duration quiet) {
        return drain(transport, subscription, Scope.whole(), minimum, timeout, quiet);
    }

    /** Like the drain above, reading the nodes in {@code scope}. */
    static List<Message> drain(
            Transport transport, ResourceId subscription, Scope scope, int minimum, Duration timeout, Duration quiet) {
        long until = System.nanoTime() + timeout.toNanos();
        List<Message> drained = new ArrayList<>();
        while (true) {
            try (Receiver receiver = transport.receiver(subscription, scope, ReceiverOptions.consume())) {
                while (true) {
                    List<Message> batch = receiver.receive(500, quiet);
                    if (batch.isEmpty()) {
                        if (drained.size() >= minimum || System.nanoTime() > until) {
                            return drained;
                        }
                        continue;
                    }
                    for (Message message : batch) {
                        try {
                            receiver.settle(message, Disposition.accept());
                            drained.add(message);
                        } catch (BrokerException stale) {
                            // Delivered again after the link recovered; it is counted then.
                        }
                    }
                }
            } catch (BrokerException e) {
                if (System.nanoTime() > until) {
                    throw e;
                }
                pause(Duration.ofMillis(250));
            }
        }
    }

    // Errors.

    /** The broker error behind a failure, unwrapping the futures' wrappers. */
    static BrokerException broker(Throwable failure) {
        Throwable cause = failure;
        while ((cause instanceof CompletionException || cause instanceof ExecutionException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        if (cause instanceof BrokerException broker) {
            return broker;
        }
        throw new AssertionError("Expected a BrokerException, got " + cause, cause);
    }
}
