package io.github.sudoitir.openbroker.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/** A cluster before Kafka 4.0 is refused at verify, with the version it reported. */
@Testcontainers
class KafkaOldClusterIT {

    @Container
    static final KafkaContainer OLD = new KafkaContainer(System.getProperty("kafka.old.image", "apache/kafka:3.9.1"));

    @Test
    void verifyRefusesAClusterBeforeKafka40() {
        ClusterSpec spec = new ClusterSpec(
                List.of(),
                List.of(Endpoint.of(Plane.DATA, KafkaOptions.PROTOCOL, OLD.getBootstrapServers())),
                Map.of());

        try (BrokerDrivers drivers = BrokerDrivers.load()) {
            assertThatThrownBy(() -> drivers.verify(new ConnectRequest(
                            KafkaDriver.SYSTEM, spec, CredentialSource.none(), ConnectOptions.defaults())))
                    .isInstanceOfSatisfying(BrokerException.class, e -> {
                        assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                        assertThat(e.reason()).isEqualTo(ErrorReasons.UNSUPPORTED_VERSION);
                        assertThat(e.getMessage())
                                .contains("before 4.0")
                                .contains("level 21")
                                .contains("[4.0.0,)");
                    });
        }
    }
}
