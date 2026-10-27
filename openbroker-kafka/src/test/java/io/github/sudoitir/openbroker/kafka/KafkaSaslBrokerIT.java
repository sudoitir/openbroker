package io.github.sudoitir.openbroker.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import org.junit.jupiter.api.Test;

/** The SASL fixture itself: the right password gets in, a wrong one is refused without being shown. */
class KafkaSaslBrokerIT {

    private static ConnectRequest as(String password) {
        return new ConnectRequest(
                KafkaDriver.SYSTEM,
                KafkaSaslBroker.spec(),
                CredentialSource.of(Credentials.usernamePassword(KafkaSaslBroker.USER, password)),
                ConnectOptions.defaults());
    }

    @Test
    void theRightPasswordGetsIn() {
        try (BrokerDrivers drivers = BrokerDrivers.load()) {
            assertThat(drivers.verify(as(KafkaSaslBroker.PASSWORD)).nodes().value())
                    .hasSize(1);
        }
    }

    @Test
    void aWrongPasswordIsRefusedAndNotShown() {
        try (BrokerDrivers drivers = BrokerDrivers.load()) {
            assertThatThrownBy(() -> drivers.verify(as("not-the-password")))
                    .isInstanceOfSatisfying(BrokerException.class, e -> {
                        assertThat(e.status()).isEqualTo(Status.UNAUTHENTICATED);
                        assertThat(e.reason()).isEqualTo(ErrorReasons.CREDENTIALS_REJECTED);
                        assertThat(e.toString()).doesNotContain("not-the-password");
                    });
        }
    }
}
