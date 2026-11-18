package io.github.sudoitir.openbroker.kafka.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.kafka.KafkaOptions;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;

/** Tiered offsets: an unknown offset is a reason, and a broker that rejects the spec is not asked again. */
class TieredOffsetsTest {

    private static final TopicPartition ORDERS = new TopicPartition("orders", 0);
    private static final Instant NOW = Instant.parse("2026-11-18T12:00:00Z");

    @Test
    void aTopicThatIsNotTieredHasNoTieredNumber() {
        Attributes.Builder attributes = Attributes.builder();

        TieredOffsets.put(attributes, Map.of(ORDERS, 0L), Map.of(ORDERS, -1L), ORDERS);

        Attributes built = attributes.build();
        assertThat(built.raw(KafkaKinds.LOCAL_LOG_START_OFFSET)).contains(0L);
        assertThat(built.raw(KafkaKinds.TIERED_OFFSET)).isEmpty();
        assertThat(built.raw(KafkaKinds.TIERED_OFFSET_REASON)).contains(TieredOffsets.NOT_TIERED);
    }

    @Test
    void aTieredOffsetIsTheNumberTheBrokerReturned() {
        Attributes.Builder attributes = Attributes.builder();

        TieredOffsets.put(attributes, Map.of(ORDERS, 4L), Map.of(ORDERS, 12L), ORDERS);

        assertThat(attributes.build().raw(KafkaKinds.TIERED_OFFSET)).contains(12L);
        assertThat(attributes.build().raw(KafkaKinds.TIERED_OFFSET_REASON)).isEmpty();
    }

    @Test
    void aMissingOffsetIsNotInvented() {
        Attributes.Builder attributes = Attributes.builder();

        TieredOffsets.put(attributes, Map.of(), Map.of(), ORDERS);

        assertThat(attributes.build().asMap()).isEmpty();
    }

    @Test
    void anUnsupportedSpecIsUnavailableWithTheBrokersReasonAndIsNotRetried() {
        Admin admin = mock(Admin.class);
        when(admin.listOffsets(anyMap(), any(ListOffsetsOptions.class))).thenReturn(rejected("ListOffsets v9"));
        KafkaCluster cluster = cluster();
        Context context = new Context(cluster, null, admin, await(), NOW, null);

        assertThat(TieredOffsets.fetch(context, List.of(ORDERS))).isEqualTo(TieredOffsets.Read.none());
        assertThat(TieredOffsets.fetch(context, List.of(ORDERS))).isEqualTo(TieredOffsets.Read.none());

        Assessment decided = cluster.decided(KafkaAccess.TIERED_OFFSETS).orElseThrow();
        assertThat(decided.status()).isEqualTo(Assessment.Status.UNAVAILABLE);
        assertThat(decided.reason()).contains("ListOffsets v9");
        verify(admin, times(1)).listOffsets(anyMap(), any(ListOffsetsOptions.class));
        assertThat(cluster.decide(KafkaAccess.TIERED_OFFSETS, Assessment.available("later"))
                        .status())
                .isEqualTo(Assessment.Status.UNAVAILABLE);
    }

    @Test
    void aBrokerThatRejectsTheProbeIsUnavailableWithItsReason() {
        Admin admin = mock(Admin.class);
        when(admin.listOffsets(anyMap(), any(ListOffsetsOptions.class))).thenReturn(rejected("no tiered offsets"));

        Assessment assessment = ClusterFeatures.assessTieredOffsets(admin, await());

        assertThat(assessment.status()).isEqualTo(Assessment.Status.UNAVAILABLE);
        assertThat(assessment.reason()).contains("no tiered offsets");
        verify(admin, times(1)).listOffsets(anyMap(), any(ListOffsetsOptions.class));
    }

    private static ListOffsetsResult rejected(String reason) {
        KafkaFutureImpl<ListOffsetsResult.ListOffsetsResultInfo> failed = new KafkaFutureImpl<>();
        failed.completeExceptionally(new UnsupportedVersionException(reason));
        return new ListOffsetsResult(Map.of(ORDERS, failed));
    }

    private static Await await() {
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        return new Await(Deadline.after(Duration.ofSeconds(5), clock), clock, NodeId.of("cluster"));
    }

    private static KafkaCluster cluster() {
        ClusterSpec spec = new ClusterSpec(
                List.of(), List.of(Endpoint.of(Plane.DATA, KafkaOptions.PROTOCOL, "127.0.0.1:1")), Map.of());
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
        return new KafkaCluster(spec, CredentialSource.none(), context);
    }
}
