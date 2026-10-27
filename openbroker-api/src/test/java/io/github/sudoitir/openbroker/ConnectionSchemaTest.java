package io.github.sudoitir.openbroker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.groups.Tuple.tuple;

import io.github.sudoitir.openbroker.ConnectionSchema.Problem;
import io.github.sudoitir.openbroker.ConnectionSchema.Violation;
import io.github.sudoitir.openbroker.spi.BrokerDriver;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ConnectionSchemaTest {

    private static final ConnectionSchema SCHEMA = new ConnectionSchema(List.of(
            option("mode", OptionSpec.Type.STRING, false, false, List.of("fast", "safe")),
            option("retries", OptionSpec.Type.INTEGER, false, false, List.of()),
            option("watch", OptionSpec.Type.DURATION, false, false, List.of()),
            option("strict", OptionSpec.Type.BOOLEAN, false, false, List.of()),
            option("registry", OptionSpec.Type.URI, false, false, List.of()),
            option("region", OptionSpec.Type.STRING, true, false, List.of()),
            option("pin", OptionSpec.Type.INTEGER, false, true, List.of())));

    @Test
    void optionsThatFitHaveNoViolations() {
        assertThat(SCHEMA.validate(Map.of(
                        "region", "eu",
                        "mode", "safe",
                        "retries", "3",
                        "watch", "PT10S",
                        "strict", "TRUE",
                        "registry", "https://registry.example/x",
                        "pin", "1234")))
                .isEmpty();
    }

    @Test
    void reportsEveryKindOfViolation() {
        List<Violation> violations = SCHEMA.validate(Map.of(
                "mode", "turbo",
                "retries", "three",
                "watch", "10s",
                "strict", "yes",
                "registry", "not a uri",
                "mdoe", "fast"));

        assertThat(violations)
                .extracting(Violation::key, Violation::problem)
                .containsExactlyInAnyOrder(
                        tuple("mode", Problem.NOT_ALLOWED),
                        tuple("retries", Problem.WRONG_TYPE),
                        tuple("watch", Problem.WRONG_TYPE),
                        tuple("strict", Problem.WRONG_TYPE),
                        tuple("registry", Problem.WRONG_TYPE),
                        tuple("mdoe", Problem.UNKNOWN),
                        tuple("region", Problem.MISSING));
    }

    @Test
    void aViolationNeverQuotesTheValue() {
        List<Violation> violations = SCHEMA.validate(Map.of("region", "eu", "pin", "s3cr3t-value"));

        assertThat(violations).singleElement().satisfies(v -> {
            assertThat(v.problem()).isEqualTo(Problem.WRONG_TYPE);
            assertThat(v.toString()).doesNotContain("s3cr3t-value");
            assertThat(v.detail()).doesNotContain("s3cr3t-value");
        });
    }

    @Test
    void connectRefusesOptionsTheDriverDoesNotDeclareBeforeCallingIt() {
        FakeDriver driver = new FakeDriver();
        try (BrokerDrivers drivers = BrokerDrivers.of(List.of(driver))) {
            ConnectRequest request = new ConnectRequest(
                    "fake",
                    new ClusterSpec(List.of(), List.of(), Map.of("region", "eu", "pin", "s3cr3t-value", "mdoe", "x")),
                    CredentialSource.none(),
                    ConnectOptions.defaults());

            assertThatThrownBy(() -> drivers.connect(request)).isInstanceOfSatisfying(BrokerException.class, e -> {
                assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT);
                assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
                assertThat(e.info().reason()).isEqualTo(ErrorReasons.INVALID_ARGUMENT);
                assertThat(e.info().metadata()).containsEntry(ErrorReasons.OPTIONS, "mdoe,pin");
                assertThat(e.getMessage()).contains("mdoe", "pin").doesNotContain("s3cr3t-value");
            });
            assertThatThrownBy(() -> drivers.verify(request)).isInstanceOf(BrokerException.class);
            assertThat(driver.calls).isZero();
        }
    }

    private static OptionSpec option(
            String key, OptionSpec.Type type, boolean required, boolean sensitive, List<String> allowed) {
        return new OptionSpec(key, type, required, sensitive, Optional.empty(), allowed, key, Optional.empty());
    }

    private static final class FakeDriver implements BrokerDriver {
        int calls;

        @Override
        public BrokerSystem system() {
            return new BrokerSystem("fake", "Fake", "1", 1, Set.of(), List.of());
        }

        @Override
        public ConnectionSchema schema() {
            return SCHEMA;
        }

        @Override
        public ConnectionReport verify(ClusterSpec cluster, CredentialSource credentials, DriverContext context) {
            calls++;
            throw new AssertionError("verify not reached");
        }

        @Override
        public ClusterConnection connect(ClusterSpec cluster, CredentialSource credentials, DriverContext context) {
            calls++;
            throw new AssertionError("connect not reached");
        }
    }
}
