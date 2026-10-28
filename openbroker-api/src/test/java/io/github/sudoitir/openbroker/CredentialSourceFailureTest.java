package io.github.sudoitir.openbroker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.spi.BrokerDriver;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * A driver asks the credential source on its own threads and retries on a {@link BrokerException}; a
 * source that fails any other way would end that thread. The source a driver is given reports such a
 * failure as {@code UNAVAILABLE}, and never quotes it.
 */
class CredentialSourceFailureTest {

    @Test
    void aFailingSourceIsReportedAsUnavailableWithoutItsText() {
        CapturingDriver driver = new CapturingDriver();
        CredentialSource failing = plane -> {
            throw new IllegalStateException("store unreachable: detail-not-to-quote");
        };
        try (BrokerDrivers drivers = BrokerDrivers.of(List.of(driver))) {
            drivers.connect(request(failing));

            assertThatThrownBy(() -> driver.credentials.forPlane(Plane.DATA))
                    .isInstanceOfSatisfying(BrokerException.class, e -> {
                        assertThat(e.status()).isEqualTo(Status.UNAVAILABLE);
                        assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
                        assertThat(e.info().reason()).isEqualTo(ErrorReasons.UNREACHABLE);
                        assertThat(e.getMessage())
                                .contains("DATA", "IllegalStateException")
                                .doesNotContain("detail-not-to-quote");
                        assertThat(e.getCause()).isInstanceOf(IllegalStateException.class);
                    });
        }
    }

    @Test
    void aBrokerExceptionFromTheSourceIsPassedOnAsItIs() {
        CapturingDriver driver = new CapturingDriver();
        BrokerException refused = new BrokerException(
                Status.UNAUTHENTICATED,
                ErrorInfo.of(ErrorReasons.CREDENTIALS_REJECTED),
                Dispatch.NOT_SENT,
                null,
                "The account was rejected.");
        try (BrokerDrivers drivers = BrokerDrivers.of(List.of(driver))) {
            drivers.verify(request(plane -> {
                throw refused;
            }));

            assertThatThrownBy(() -> driver.credentials.forPlane(Plane.MANAGEMENT))
                    .isSameAs(refused);
        }
    }

    @Test
    void aWorkingSourceAnswersAsItWould() {
        CapturingDriver driver = new CapturingDriver();
        Credentials account = Credentials.usernamePassword("app", "not-a-secret");
        try (BrokerDrivers drivers = BrokerDrivers.of(List.of(driver))) {
            drivers.connect(request(CredentialSource.of(account)));

            assertThat(driver.credentials.forPlane(Plane.DATA)).isEqualTo(account);
        }
    }

    private static ConnectRequest request(CredentialSource credentials) {
        return new ConnectRequest(
                "fake", new ClusterSpec(List.of(), List.of(), Map.of()), credentials, ConnectOptions.defaults());
    }

    private static final class CapturingDriver implements BrokerDriver {
        CredentialSource credentials;

        @Override
        public BrokerSystem system() {
            return new BrokerSystem("fake", "Fake", "1", 1, Set.of(), List.of());
        }

        @Override
        public ConnectionSchema schema() {
            return new ConnectionSchema(List.of());
        }

        @Override
        public ConnectionReport verify(ClusterSpec cluster, CredentialSource credentials, DriverContext context) {
            this.credentials = credentials;
            return null;
        }

        @Override
        public ClusterConnection connect(ClusterSpec cluster, CredentialSource credentials, DriverContext context) {
            this.credentials = credentials;
            return null;
        }
    }
}
