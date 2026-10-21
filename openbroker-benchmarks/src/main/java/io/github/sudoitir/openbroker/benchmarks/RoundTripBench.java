package io.github.sudoitir.openbroker.benchmarks;

import io.github.sudoitir.openbroker.messaging.Blob;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.Processor;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/** The time from a send to the handler having the message, one message at a time (sampled; the gate is p99). */
@State(org.openjdk.jmh.annotations.Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RoundTripBench {

    private EmbeddedBroker broker;
    private Sender sender;
    private Processor processor;
    private OutboundMessage message;
    private final Semaphore delivered = new Semaphore(0);

    @Setup
    public void setUp() throws Exception {
        broker = new EmbeddedBroker(MessagingObserver.none());
        ResourceId queue = broker.queue("round-trip");
        sender = broker.transport()
                .sender(
                        SendTarget.subscription(queue),
                        Scope.whole(),
                        SenderOptions.defaults().withGuarantee(Guarantee.AT_LEAST_ONCE));
        processor = broker.transport()
                .process(queue, Scope.whole(), ProcessorOptions.defaults().withConcurrency(1), context -> {
                    delivered.release();
                    return Disposition.accept();
                });
        message = OutboundMessage.of(MessageBody.of(Blob.of(new byte[1024])));
    }

    @TearDown
    public void tearDown() throws Exception {
        processor.close();
        sender.close();
        broker.close();
    }

    @Benchmark
    public void sendToHandler() throws InterruptedException {
        sender.send(message);
        delivered.acquire();
    }
}
