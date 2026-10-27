package io.github.sudoitir.openbroker.kafka.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.KafkaDriver;
import io.github.sudoitir.openbroker.kafka.KafkaTestCluster;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import org.junit.jupiter.api.Test;

/** Kafka 4.0 has no share groups: the kind is not declared, and opening one says why and what to do. */
class KafkaShareAbsentIT {

    @Test
    void theShareKindIsAbsentFromAnOldClusterAndOpeningOneNamesTheUpgrade() {
        try (KafkaTestCluster old = KafkaTestCluster.start("apache/kafka:4.0.2");
                BrokerDrivers drivers = BrokerDrivers.load();
                ClusterConnection connection = drivers.connect(new ConnectRequest(
                        KafkaDriver.SYSTEM, old.spec(), CredentialSource.none(), ConnectOptions.defaults()))) {
            var transport = connection.transport().orElseThrow();
            assertThat(transport.capabilities().subscriptions()).containsOnlyKeys(Subscriptions.GROUP_TOPIC);
            assertThatThrownBy(() -> transport.process(
                            ResourceId.of("kafka.share-group-topic", "g", "t"),
                            Scope.whole(),
                            ProcessorOptions.defaults(),
                            ctx -> Disposition.accept()))
                    .isInstanceOfSatisfying(BrokerException.class, e -> {
                        assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED);
                        assertThat(e.getMessage()).contains("4.2").contains("share.version");
                    });
        }
    }
}
