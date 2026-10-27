package io.github.sudoitir.openbroker.benchmarks;

import io.github.sudoitir.openbroker.messaging.Blob;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.Offer;
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

/**
 * A processor of concurrency 8 under AT_LEAST_ONCE. One operation is one message sent and handled:
 * at most {@value #WINDOW} are in flight, so the rate is the processor's and the allocation per
 * operation covers the send and the receive side.
 */
@State(org.openjdk.jmh.annotations.Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class KafkaProcessBench {

    private static final int WINDOW = 256;

    private KafkaBroker broker;
    private Sender sender;
    private Processor processor;
    private OutboundMessage message;
    private final Semaphore window = new Semaphore(WINDOW);

    @Setup
    public void setUp() {
        broker = new KafkaBroker();
        ResourceId topic = broker.topic("process");
        sender = broker.transport()
                .sender(
                        SendTarget.subscription(topic),
                        Scope.whole(),
                        SenderOptions.defaults().withGuarantee(Guarantee.AT_LEAST_ONCE));
        processor = broker.transport()
                .process(topic, Scope.whole(), ProcessorOptions.defaults().withConcurrency(8), context -> {
                    window.release();
                    return Disposition.accept();
                });
        message = OutboundMessage.of(MessageBody.of(Blob.of(new byte[1024])));
    }

    @TearDown
    public void tearDown() {
        processor.close();
        sender.close();
        broker.close();
    }

    @Benchmark
    public void sendAndProcess() throws InterruptedException {
        window.acquire();
        while (sender.offer(message) instanceof Offer.Full) {
            Thread.onSpinWait();
        }
    }
}
