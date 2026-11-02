package io.github.sudoitir.openbroker.kafka.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Operation;
import io.github.sudoitir.openbroker.operation.Preview;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The broker-configuration handler against an Admin client that answers as a cluster would. */
class BrokerConfigsTest {

    private static final Instant NOW = Instant.parse("2026-11-02T12:00:00Z");
    private static final ConfigResource DEFAULT = new ConfigResource(ConfigResource.Type.BROKER, "");
    private static final ConfigResource BROKER_1 = new ConfigResource(ConfigResource.Type.BROKER, "1");
    private static final ConfigResource BROKER_2 = new ConfigResource(ConfigResource.Type.BROKER, "2");

    private final Admin admin = mock(Admin.class);
    private final BrokerConfigs handler = new BrokerConfigs(KafkaKinds.base().stream()
            .filter(k -> k.id().equals(KafkaKinds.BROKER_CONFIG))
            .findFirst()
            .orElseThrow());

    private static ConfigEntry entry(
            String name, String value, ConfigEntry.ConfigSource source, boolean sensitive, boolean readOnly) {
        return new ConfigEntry(
                name, value, source, sensitive, readOnly, List.of(), ConfigEntry.ConfigType.STRING, null);
    }

    private static ConfigEntry dynamic(String name, String value) {
        return entry(name, value, ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG, false, false);
    }

    private Context context() {
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        return new Context(
                null,
                null,
                admin,
                new Await(Deadline.after(Duration.ofSeconds(30), clock), clock, NodeId.of("cluster")),
                NOW,
                null);
    }

    @BeforeEach
    void aClusterOfTwoBrokers() {
        DescribeClusterResult described = mock(DescribeClusterResult.class);
        when(described.nodes())
                .thenReturn(KafkaFuture.completedFuture(List.of(new Node(1, "b1", 9092), new Node(2, "b2", 9092))));
        when(admin.describeCluster(any())).thenReturn(described);
        DescribeConfigsResult configs = mock(DescribeConfigsResult.class);
        when(configs.all())
                .thenReturn(KafkaFuture.completedFuture(Map.of(
                        DEFAULT,
                        new Config(List.of(dynamic("message.max.bytes", "2097152"))),
                        BROKER_1,
                        broker(),
                        BROKER_2,
                        broker())));
        when(admin.describeConfigs(anyCollection(), any())).thenReturn(configs);
    }

    private static Config broker() {
        return new Config(List.of(
                entry(
                        "message.max.bytes",
                        "2097152",
                        ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG,
                        false,
                        false),
                entry("log.cleaner.threads", "1", ConfigEntry.ConfigSource.DEFAULT_CONFIG, false, false),
                entry("log.dirs", "/var/kafka", ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG, false, true),
                entry("ssl.keystore.password", null, ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG, true, false)));
    }

    private Listing everything() {
        return new Listing(ResourceQuery.of(KafkaKinds.BROKER_CONFIG));
    }

    @Test
    void theListingHoldsTheDefaultAndEachBrokerAndReadsThemWithOneCall() {
        List<Candidate> found = handler.candidates(context(), everything());
        List<Resource> resources = handler.resources(context(), found, everything());

        assertThat(found).extracting(c -> c.id().name()).containsExactly("*", "1", "2");
        assertThat(resources).hasSize(3);
        verify(admin, times(1)).describeConfigs(anyCollection(), any());
    }

    @Test
    void settingsAreAttributesExceptSecretsAndTheRestartOnesAndSourcesAreNamed() {
        List<Resource> resources =
                handler.resources(context(), handler.candidates(context(), everything()), everything());

        Resource one = resources.get(1);
        assertThat(one.attributes().raw(KafkaKinds.CONFIG_PREFIX + "log.cleaner.threads"))
                .contains("1");
        assertThat(one.attributes().asMap()).doesNotContainKey(KafkaKinds.CONFIG_PREFIX + "ssl.keystore.password");
        assertThat(one.attributes().raw(KafkaKinds.CONFIG_READ_ONLY)).contains("log.dirs");
        assertThat(one.attributes().raw(KafkaKinds.CONFIG_SOURCES))
                .contains("log.dirs=static,message.max.bytes=cluster");
        assertThat(resources.getFirst().attributes().asMap()).doesNotContainKey(KafkaKinds.CONFIG_READ_ONLY);
    }

    @Test
    void anUpdatePreviewShowsBeforeAndAfterOfTheKeysItPatches() {
        Preview preview = handler.preview(
                context(),
                BrokerConfigs.id("*"),
                new Operation.Update(Attributes.builder()
                        .putRaw(KafkaKinds.CONFIG_PREFIX + "message.max.bytes", "4194304")
                        .build()),
                Scope.whole());

        assertThat(preview.diff()).singleElement().satisfies(change -> {
            assertThat(change.key()).isEqualTo(KafkaKinds.CONFIG_PREFIX + "message.max.bytes");
            assertThat(change.before()).contains("2097152");
            assertThat(change.after()).contains("4194304");
        });
        assertThat(preview.etag()).isNotBlank();
    }

    @Test
    void aReadOnlyKeyIsRefusedWithoutWritingAndSaysToRestart() {
        Operation update = new Operation.Update(Attributes.builder()
                .putRaw(KafkaKinds.CONFIG_PREFIX + "log.dirs", "/data")
                .build());

        assertThatThrownBy(() -> handler.execute(context(), BrokerConfigs.id("1"), update, Scope.whole()))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                    assertThat(e.reason()).isEqualTo(ErrorReasons.IMMUTABLE_FIELD);
                    assertThat(e.getMessage()).contains("server.properties").contains("Restart");
                });
        verify(admin, never()).incrementalAlterConfigs(anyMap(), any());
    }

    @Test
    void aReadOnlyKeyOfTheDefaultIsRefusedToo() {
        Operation update = new Operation.Update(Attributes.builder()
                .putRaw(KafkaKinds.CONFIG_PREFIX + "log.dirs", "/data")
                .build());

        assertThatThrownBy(() -> handler.preview(context(), BrokerConfigs.id("*"), update, Scope.whole()))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.reason()).isEqualTo(ErrorReasons.IMMUTABLE_FIELD));
        verify(admin, never()).incrementalAlterConfigs(anyMap(), any());
    }

    @Test
    void aSensitiveKeyIsRefusedAsInvalidWithoutQuotingTheValue() {
        Operation update = new Operation.Update(Attributes.builder()
                .putRaw(KafkaKinds.CONFIG_PREFIX + "ssl.keystore.password", "hunter2")
                .build());

        assertThatThrownBy(() -> handler.execute(context(), BrokerConfigs.id("1"), update, Scope.whole()))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT);
                    assertThat(e.getMessage()).doesNotContain("hunter2");
                });
        verify(admin, never()).incrementalAlterConfigs(anyMap(), any());
    }

    @Test
    void aBrokerTheClusterDoesNotHaveIsGone() {
        assertThat(handler.find(context(), BrokerConfigs.id("9"))).isEmpty();
    }
}
