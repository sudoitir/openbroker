package io.github.sudoitir.openbroker.support.messaging;

import static io.github.sudoitir.openbroker.support.messaging.Fakes.delivered;
import static io.github.sudoitir.openbroker.support.messaging.Fakes.outbound;
import static io.github.sudoitir.openbroker.support.messaging.Fakes.receipt;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.CallInterceptor;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import io.github.sudoitir.openbroker.messaging.OperationType;
import io.github.sudoitir.openbroker.messaging.OrderingScope;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.Recovery;
import io.github.sudoitir.openbroker.messaging.RetryPolicy;
import io.github.sudoitir.openbroker.messaging.SendBuffer;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.support.messaging.Fakes.FakeClock;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The engine blocks on locks and conditions, never in a way that pins a virtual thread's carrier. */
class PinningTest {

    private static final int MESSAGES = 300;

    @Test
    void sendingRecoveringAndProcessingPinNoVirtualThread(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("pinned.jfr");
        // Class initialisation pins by design when virtual threads race it; run once to load every class first.
        workload();
        try (Recording recording = new Recording()) {
            recording.enable("jdk.VirtualThreadPinned").withThreshold(Duration.ZERO);
            recording.start();
            workload();
            recording.stop();
            recording.dump(file);
        }
        List<RecordedEvent> pinned = RecordingFile.readAllEvents(file).stream()
                .filter(e -> e.getEventType().getName().equals("jdk.VirtualThreadPinned"))
                .toList();
        assertThat(pinned).as("pinned virtual threads: %s", pinned).isEmpty();
    }

    private static void workload() throws Exception {
        FakeClock clock = new FakeClock();
        EngineContext context = new EngineContext(
                clock, new Random(3), Sleeper.system(), task -> task, MessagingObserver.none(), CallInterceptor.none());
        NodeId node = Fakes.NODE;

        LinkedBlockingQueue<SendPipeline.Confirmation> acks = new LinkedBlockingQueue<>();
        SendPipeline pipeline = SendPipeline.open(
                "pin",
                node,
                SenderOptions.defaults().withBuffer(new SendBuffer(8, 1 << 20, Duration.ofSeconds(10))),
                Fakes.info(OperationType.SEND),
                context,
                (message, confirmation) -> acks.add(confirmation),
                cause -> {});
        Thread confirmer = Thread.ofPlatform().start(() -> {
            try {
                for (int i = 0; i < MESSAGES; i++) {
                    acks.take().confirmed(receipt("m" + i));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        List<CompletableFuture<Void>> senders = new ArrayList<>();
        for (int s = 0; s < 3; s++) {
            senders.add(CompletableFuture.runAsync(
                    () -> {
                        for (int i = 0; i < MESSAGES / 3; i++) {
                            pipeline.send(outbound("x"));
                        }
                    },
                    task -> Thread.ofVirtual().start(task)));
        }
        for (CompletableFuture<Void> sender : senders) {
            sender.get(30, TimeUnit.SECONDS);
        }
        pipeline.flush(Duration.ofSeconds(10));
        confirmer.join();
        pipeline.close();

        LinkSupervisor supervisor = new LinkSupervisor("pin", node, "e", Recovery.defaults(), () -> node, context);
        LinkedBlockingQueue<LinkState> states = new LinkedBlockingQueue<>();
        supervisor.listen(event -> states.add(event.state()));
        supervisor.lost(new BrokerException(
                Status.UNAVAILABLE, ErrorInfo.of(ErrorReasons.UNREACHABLE), Dispatch.IN_DOUBT, node, "lost"));
        assertThat(states.poll(10, TimeUnit.SECONDS)).isEqualTo(LinkState.RECOVERING);
        assertThat(states.poll(10, TimeUnit.SECONDS)).isEqualTo(LinkState.OPEN);
        supervisor.close();

        LinkedBlockingQueue<Message> inbox = new LinkedBlockingQueue<>();
        AtomicInteger settled = new AtomicInteger();
        ProcessorEngine.Lanes lanes = new ProcessorEngine.Lanes() {
            @Override
            public ProcessorEngine.Lane open(int index) {
                return new ProcessorEngine.Lane() {
                    @Override
                    public Optional<Message> next(Duration timeout) throws InterruptedException {
                        return Optional.ofNullable(inbox.poll(timeout.toNanos(), TimeUnit.NANOSECONDS));
                    }

                    @Override
                    public void settle(Message message, Disposition disposition) {
                        settled.incrementAndGet();
                    }

                    @Override
                    public Sender sender(SendTarget target) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void close() {}
                };
            }

            @Override
            public boolean pinsGroups() {
                return false;
            }

            @Override
            public LinkState state() {
                return LinkState.OPEN;
            }

            @Override
            public Sender deadLetter(SendTarget target) {
                throw new UnsupportedOperationException();
            }
        };
        ProcessorEngine engine = ProcessorEngine.start(
                "pin",
                ProcessorOptions.defaults()
                        .withConcurrency(4)
                        .withOrdering(OrderingScope.GROUP)
                        .withRetry(RetryPolicy.system()),
                Fakes.info(OperationType.PROCESS),
                context,
                lanes,
                ctx -> Disposition.accept());
        for (int i = 0; i < MESSAGES; i++) {
            inbox.add(delivered("m" + i, "g" + (i % 5), 0));
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (settled.get() < MESSAGES && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        engine.stop(Duration.ofSeconds(5));
        assertThat(settled.get()).isEqualTo(MESSAGES);
    }
}
