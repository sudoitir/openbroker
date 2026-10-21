package io.github.sudoitir.openbroker.benchmarks;

import io.github.sudoitir.openbroker.messaging.Blob;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import io.github.sudoitir.openbroker.messaging.Observation;
import io.github.sudoitir.openbroker.messaging.OperationInfo;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.SendReceipt;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import org.jspecify.annotations.Nullable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * The same send with {@link MessagingObserver#none()} and with an observer that counts. The
 * observer is the only difference, so {@code none()} must add no bytes to an operation.
 */
@State(org.openjdk.jmh.annotations.Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ObserverBench {

    private static final String QUEUE = "observer";

    private static final class Counting implements MessagingObserver {
        private final LongAdder sends = new LongAdder();
        private final Observation observation = new Observation() {
            @Override
            public void end(@Nullable Throwable error) {}
        };

        @Override
        public Observation onSend(OperationInfo op, OutboundMessage message, BiConsumer<String, String> carrier) {
            sends.increment();
            return observation;
        }

        @Override
        public Observation onReceive(OperationInfo op) {
            return observation;
        }

        @Override
        public Observation onProcess(OperationInfo op, Message message) {
            return observation;
        }

        @Override
        public Observation onSettle(OperationInfo op, Message message, Disposition disposition) {
            return observation;
        }
    }

    private EmbeddedBroker noneBroker;
    private EmbeddedBroker countingBroker;
    private Sender none;
    private Sender counting;
    private OutboundMessage message;

    @Setup
    public void setUp() throws Exception {
        noneBroker = new EmbeddedBroker(MessagingObserver.none());
        countingBroker = new EmbeddedBroker(new Counting());
        none = sender(noneBroker);
        counting = sender(countingBroker);
        message = OutboundMessage.of(MessageBody.of(Blob.of(new byte[1024])));
    }

    @Setup(Level.Iteration)
    public void purge() throws Exception {
        noneBroker.purge(QUEUE);
        countingBroker.purge(QUEUE);
    }

    @TearDown
    public void tearDown() throws Exception {
        none.close();
        counting.close();
        noneBroker.close();
        countingBroker.close();
    }

    @Benchmark
    public SendReceipt sendWithoutObserver() {
        return none.send(message);
    }

    @Benchmark
    public SendReceipt sendWithCountingObserver() {
        return counting.send(message);
    }

    private static Sender sender(EmbeddedBroker broker) throws Exception {
        ResourceId queue = broker.queue(QUEUE);
        return broker.transport()
                .sender(
                        SendTarget.subscription(queue),
                        Scope.whole(),
                        SenderOptions.defaults().withGuarantee(Guarantee.AT_MOST_ONCE));
    }
}
