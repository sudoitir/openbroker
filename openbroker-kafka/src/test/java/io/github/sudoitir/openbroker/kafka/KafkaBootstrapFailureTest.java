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
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** A cluster that cannot be reached is classified at once, with no broker to ask. */
class KafkaBootstrapFailureTest {

    private static ConnectRequest request(String address) {
        return new ConnectRequest(
                KafkaDriver.SYSTEM,
                new ClusterSpec(List.of(), List.of(Endpoint.of(Plane.DATA, KafkaOptions.PROTOCOL, address)), Map.of()),
                CredentialSource.none(),
                ConnectOptions.defaults());
    }

    private static void failsFastAs(String address, String reason) {
        try (BrokerDrivers drivers = BrokerDrivers.load()) {
            long started = System.nanoTime();
            assertThatThrownBy(() -> drivers.verify(request(address)))
                    .isInstanceOfSatisfying(BrokerException.class, e -> {
                        assertThat(e.status()).isEqualTo(Status.UNAVAILABLE);
                        assertThat(e.info().metadata())
                                .containsEntry("kafka.endpoint", address)
                                .containsEntry("kafka.reach_failure", reason);
                    });
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(5));
        }
    }

    @Test
    void verifyOfAnUnresolvableBootstrapServerFailsAsUnreachableByName() {
        failsFastAs("kafka-x.invalid:9092", "dns");
    }

    @Test
    void verifyOfAClosedBootstrapPortFailsAsRefusedLongBeforeTheDeadline() throws IOException {
        int port;
        try (ServerSocket probe = new ServerSocket(0)) {
            port = probe.getLocalPort();
        }

        failsFastAs("127.0.0.1:" + port, "refused");
    }
}
