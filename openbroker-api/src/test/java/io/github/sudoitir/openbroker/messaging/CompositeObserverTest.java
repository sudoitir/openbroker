package io.github.sudoitir.openbroker.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.BiConsumer;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

class CompositeObserverTest {

    private static final OperationInfo SEND = new OperationInfo(
            "test",
            OperationType.SEND,
            "send",
            ResourceId.of("test.queue", "orders"),
            Optional.empty(),
            NodeId.of("n1"),
            Optional.empty(),
            OptionalInt.empty(),
            1);

    @Test
    void nothingToComposeIsNone() {
        assertThat(MessagingObserver.composite()).isSameAs(MessagingObserver.none());
        assertThat(MessagingObserver.composite(MessagingObserver.none(), MessagingObserver.none()))
                .isSameAs(MessagingObserver.none());
    }

    @Test
    void oneObserverIsItself() {
        Recording one = new Recording("a", false);
        assertThat(MessagingObserver.composite(MessagingObserver.none(), one)).isSameAs(one);
    }

    @Test
    void everyObserverSeesTheSendAndItsEndInOrder() {
        List<String> events = new ArrayList<>();
        Recording a = new Recording("a", false, events);
        Recording b = new Recording("b", false, events);
        Map<String, String> carrier = new HashMap<>();

        Observation observation = MessagingObserver.composite(a, b)
                .onSend(SEND, OutboundMessage.of(MessageBody.of(new byte[0])), carrier::put);
        try (var _ = observation.scope()) {
            events.add("work");
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        observation.end(null);

        assertThat(events)
                .containsExactly(
                        "a.send", "b.send", "a.open", "b.open", "work", "b.close", "a.close", "a.end", "b.end");
        assertThat(carrier).containsKeys("a", "b");
    }

    @Test
    void aFailingObserverStopsNeitherTheOtherNorTheOperation() {
        List<String> events = new ArrayList<>();
        Recording broken = new Recording("x", true, events);
        Recording fine = new Recording("b", false, events);

        Observation observation = MessagingObserver.composite(broken, fine)
                .onSend(SEND, OutboundMessage.of(MessageBody.of(new byte[0])), (k, v) -> {});
        observation.end(new IllegalStateException("send failed"));

        assertThat(events).containsExactly("b.send", "b.end");
    }

    private static final class Recording implements MessagingObserver {
        private final String name;
        private final boolean broken;
        private final List<String> events;

        Recording(String name, boolean broken) {
            this(name, broken, new ArrayList<>());
        }

        Recording(String name, boolean broken, List<String> events) {
            this.name = name;
            this.broken = broken;
            this.events = events;
        }

        @Override
        public Observation onSend(OperationInfo op, OutboundMessage message, BiConsumer<String, String> carrier) {
            if (broken) {
                throw new IllegalStateException("observer bug");
            }
            events.add(name + ".send");
            carrier.accept(name, "1");
            return observation();
        }

        @Override
        public Observation onReceive(OperationInfo op) {
            return observation();
        }

        @Override
        public Observation onProcess(OperationInfo op, Message message) {
            return observation();
        }

        @Override
        public Observation onSettle(OperationInfo op, Message message, Disposition disposition) {
            return observation();
        }

        private Observation observation() {
            return new Observation() {
                @Override
                public AutoCloseable scope() {
                    events.add(name + ".open");
                    return () -> events.add(name + ".close");
                }

                @Override
                public void end(@Nullable Throwable error) {
                    events.add(name + ".end");
                }
            };
        }
    }
}
