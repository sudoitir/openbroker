package io.github.sudoitir.openbroker.kafka.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.kafka.KafkaOptions;
import io.github.sudoitir.openbroker.kafka.admin.KafkaCluster;
import io.github.sudoitir.openbroker.kafka.codec.RecordCodec;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.spi.DriverContext;
import io.github.sudoitir.openbroker.support.messaging.ProcessorEngine;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

/** What a lane does with a record when the dead-letter send fails: it must not be left unsettled. */
class KafkaLanesTest {

    private static final ResourceId SOURCE = ResourceId.of("kafka.group-topic", "g", "orders");

    private static final class Recording implements Consuming {
        final List<String> calls = new ArrayList<>();
        final List<Duration> delays = new ArrayList<>();

        @Override
        public void start() {}

        @Override
        public LinkState state() {
            return LinkState.OPEN;
        }

        @Override
        public @Nullable BrokerException failure() {
            return null;
        }

        @Override
        public void settle(Delivery delivery) {
            calls.add("settle");
        }

        @Override
        public void reject(Delivery delivery) {
            calls.add("reject");
        }

        @Override
        public void requeue(Delivery delivery, Duration delay, boolean failed) {
            calls.add("requeue");
            delays.add(delay);
        }

        @Override
        public void close() {}
    }

    private static MessagingEnv env() {
        DriverContext context = new DriverContext() {
            @Override
            public ConnectOptions options() {
                return ConnectOptions.defaults();
            }

            @Override
            public <T extends AutoCloseable> T shared(String key, Class<T> type, Supplier<T> factory) {
                return factory.get();
            }

            @Override
            public void evict(String key) {}
        };
        ClusterSpec spec = new ClusterSpec(
                List.of(), List.of(Endpoint.of(Plane.DATA, KafkaOptions.PROTOCOL, "unused:9092")), Map.of());
        return new MessagingEnv(new KafkaCluster(spec, CredentialSource.none(), context));
    }

    private static Delivery delivery() {
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("orders", 0, 7, null, new byte[] {1});
        return new Delivery(
                RecordCodec.message(SOURCE, record, Long.MAX_VALUE, 1), record, 1, new Delivery.Assignment());
    }

    @Test
    void aRejectionWhoseDeadLetterCannotBeSentIsHandedBackNotLeftUnsettled() throws Exception {
        Recording consuming = new Recording();
        DeadLetterSink failing = new DeadLetterSink() {
            @Override
            public void reject(Delivery d, int attempts, String reason, @Nullable Function<SendTarget, Sender> joined) {
                throw new IllegalStateException("the dead-letter topic is gone");
            }

            @Override
            public Sender sender(SendTarget target) {
                throw new UnsupportedOperationException();
            }
        };
        KafkaLanes lanes = new KafkaLanes(
                env(),
                "p",
                Subscriptions.groupTopic(SOURCE),
                ProcessorOptions.defaults().withConcurrency(1),
                failing,
                null,
                false,
                sink -> consuming);
        ProcessorEngine.Lane lane = lanes.open(0);
        Delivery delivery = delivery();
        lanes.deliver(delivery);
        Message message = lane.next(Duration.ofSeconds(1)).orElseThrow();

        assertThatThrownBy(() -> lane.settle(message, new Disposition.Reject(Optional.of("bad"))))
                .isInstanceOf(IllegalStateException.class);

        assertThat(consuming.calls).containsExactly("requeue");
        assertThat(consuming.delays.getFirst()).isPositive();
        lanes.close();
    }
}
