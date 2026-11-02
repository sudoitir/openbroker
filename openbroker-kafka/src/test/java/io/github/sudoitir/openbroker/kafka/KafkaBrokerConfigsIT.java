package io.github.sudoitir.openbroker.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.admin.KafkaKinds;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.operation.Operation;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Broker configuration against a real cluster: reading it, changing a dynamic setting, and the refusals. */
class KafkaBrokerConfigsIT {

    private static final String SETTING = KafkaKinds.CONFIG_PREFIX + "message.max.bytes";

    private static KafkaTestCluster kafka;
    private static BrokerDrivers drivers;
    private static ClusterConnection connection;

    @BeforeAll
    static void start() {
        kafka = KafkaTestCluster.shared();
        drivers = BrokerDrivers.load();
        connection = Kafka.connect(drivers, kafka);
    }

    @AfterAll
    static void stop() {
        connection.close();
        drivers.close();
    }

    private static ResourceId id(String name) {
        return new ResourceId(KafkaKinds.BROKER_CONFIG, ResourcePath.of(name));
    }

    private static List<Resource> list() {
        return connection
                .resources()
                .read(ResourceQuery.of(KafkaKinds.BROKER_CONFIG))
                .orThrow()
                .items();
    }

    private static Operation update(String key, String value) {
        return new Operation.Update(Attributes.builder().putRaw(key, value).build());
    }

    @Test
    void theDefaultAndEveryBrokerAreListedWithTheirSettings() {
        List<Resource> found = list();

        assertThat(found).extracting(r -> r.id().name()).contains("*", "1");
        Resource broker = found.stream()
                .filter(r -> r.id().name().equals("1"))
                .findFirst()
                .orElseThrow();
        assertThat(broker.attributes().raw(KafkaKinds.CONFIG_PREFIX + "node.id"))
                .contains("1");
        assertThat(broker.attributes()
                        .raw(KafkaKinds.CONFIG_READ_ONLY)
                        .map(String::valueOf)
                        .orElse(""))
                .contains("log.dirs");
        assertThat(broker.attributes().asMap()).doesNotContainKey(KafkaKinds.CONFIG_PREFIX + "ssl.keystore.password");
    }

    @Test
    void aDynamicSettingOfTheClusterDefaultIsChangedAndReadBack() {
        ResourceId all = id("*");
        var preview = connection.operations().preview(all, update(SETTING, "2097152"), Scope.whole());
        assertThat(preview.diff()).singleElement().satisfies(change -> {
            assertThat(change.key()).isEqualTo(SETTING);
            assertThat(change.after()).contains("2097152");
        });
        try {
            connection
                    .operations()
                    .execute(all, update(SETTING, "2097152"), Scope.whole())
                    .orThrow();

            Resource changed = Kafka.eventually(
                    () -> list().stream()
                            .filter(r -> r.id().equals(all))
                            .findFirst()
                            .orElseThrow(),
                    r -> r.attributes().raw(SETTING).equals(Optional.of("2097152")));
            assertThat(changed.attributes()
                            .raw(KafkaKinds.CONFIG_SOURCES)
                            .map(String::valueOf)
                            .orElse(""))
                    .contains("message.max.bytes=cluster");
        } finally {
            connection
                    .operations()
                    .execute(all, update(SETTING, ""), Scope.whole())
                    .orThrow();
        }
        Kafka.eventually(
                () -> list().stream()
                        .filter(r -> r.id().equals(all))
                        .findFirst()
                        .orElseThrow(),
                r -> r.attributes().raw(SETTING).isEmpty());
    }

    @Test
    void aSettingOnlyARestartChangesIsRefusedBeforeAnythingIsSent() {
        for (String broker : List.of("*", "1")) {
            assertThatThrownBy(() -> connection
                            .operations()
                            .execute(id(broker), update(KafkaKinds.CONFIG_PREFIX + "log.dirs", "/tmp/x"), Scope.whole())
                            .orThrow())
                    .isInstanceOfSatisfying(BrokerException.class, e -> {
                        assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                        assertThat(e.reason()).isEqualTo(ErrorReasons.IMMUTABLE_FIELD);
                        assertThat(e.getMessage()).contains("server.properties");
                    });
        }
    }

    @Test
    void aSettingThatHoldsASecretIsRefused() {
        assertThatThrownBy(() -> connection
                        .operations()
                        .execute(
                                id("1"), update(KafkaKinds.CONFIG_PREFIX + "ssl.keystore.password", "x"), Scope.whole())
                        .orThrow())
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT));
    }
}
