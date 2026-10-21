package io.github.sudoitir.openbroker.benchmarks;

import io.github.sudoitir.openbroker.messaging.Blob;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import io.github.sudoitir.openbroker.messaging.Offer;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * A 1 KiB message sent: {@code send}, and {@code offer} followed by its confirmation, measure the
 * latency of one; the pipelined variants offer {@value #BATCH} before waiting, which is what the
 * bounded send buffer is for.
 */
@State(org.openjdk.jmh.annotations.Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class SendBench {

    private static final int BATCH = 1000;
    private static final String QUEUE = "send";

    private EmbeddedBroker broker;
    private Sender atMostOnce;
    private Sender atLeastOnce;
    private OutboundMessage message;

    @Setup
    public void setUp() throws Exception {
        broker = new EmbeddedBroker(MessagingObserver.none());
        ResourceId queue = broker.queue(QUEUE);
        atMostOnce = broker.transport()
                .sender(
                        SendTarget.subscription(queue),
                        Scope.whole(),
                        SenderOptions.defaults().withGuarantee(Guarantee.AT_MOST_ONCE));
        atLeastOnce = broker.transport()
                .sender(
                        SendTarget.subscription(queue),
                        Scope.whole(),
                        SenderOptions.defaults()
                                .withGuarantee(Guarantee.AT_LEAST_ONCE)
                                .withIdempotent(true));
        message = OutboundMessage.of(MessageBody.of(Blob.of(new byte[1024])));
    }

    @Setup(Level.Iteration)
    public void purge() throws Exception {
        broker.purge(QUEUE);
    }

    @TearDown
    public void tearDown() throws Exception {
        atMostOnce.close();
        atLeastOnce.close();
        broker.close();
    }

    @Benchmark
    public SendReceipt sendAtMostOnce() {
        return atMostOnce.send(message);
    }

    @Benchmark
    public SendReceipt sendAtLeastOnceIdempotent() {
        return atLeastOnce.send(message);
    }

    @Benchmark
    public SendReceipt offerAtMostOnce() {
        return confirmed(atMostOnce.offer(message));
    }

    @Benchmark
    public SendReceipt offerAtLeastOnceIdempotent() {
        return confirmed(atLeastOnce.offer(message));
    }

    @Benchmark
    @OperationsPerInvocation(BATCH)
    public int pipelinedAtMostOnce() {
        return pipelined(atMostOnce);
    }

    @Benchmark
    @OperationsPerInvocation(BATCH)
    public int pipelinedAtLeastOnceIdempotent() {
        return pipelined(atLeastOnce);
    }

    /** Offers {@value #BATCH} messages, waiting on the oldest receipt whenever the buffer is full, then flushes. */
    private int pipelined(Sender sender) {
        ArrayDeque<CompletionStage<SendReceipt>> pending = new ArrayDeque<>();
        for (int i = 0; i < BATCH; i++) {
            while (true) {
                if (sender.offer(message) instanceof Offer.Accepted accepted) {
                    pending.add(accepted.receipt());
                    break;
                }
                CompletionStage<SendReceipt> oldest = pending.poll();
                if (oldest == null) {
                    Thread.onSpinWait();
                } else {
                    oldest.toCompletableFuture().join();
                }
            }
        }
        sender.flush(Duration.ofSeconds(30));
        return pending.size();
    }

    private static SendReceipt confirmed(Offer offer) {
        return switch (offer) {
            case Offer.Accepted accepted ->
                accepted.receipt().toCompletableFuture().join();
            case Offer.Full full -> throw new IllegalStateException("The send buffer is full: " + full);
        };
    }
}
