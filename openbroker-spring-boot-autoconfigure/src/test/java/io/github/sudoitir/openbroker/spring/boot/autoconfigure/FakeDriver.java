package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.BrokerSystem;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectionReport;
import io.github.sudoitir.openbroker.ConnectionSchema;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.OptionSpec;
import io.github.sudoitir.openbroker.spi.BrokerDriver;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** A driver that declares options and connects to nothing: for the tests of binding and validation. */
class FakeDriver implements BrokerDriver {

    static final String SYSTEM = "fake";

    static final ConnectionSchema SCHEMA = new ConnectionSchema(List.of(
            new OptionSpec(
                    "mode",
                    OptionSpec.Type.STRING,
                    false,
                    false,
                    Optional.empty(),
                    List.of("a", "b"),
                    "mode",
                    Optional.empty()),
            new OptionSpec(
                    "pin",
                    OptionSpec.Type.INTEGER,
                    false,
                    true,
                    Optional.empty(),
                    List.of(),
                    "a secret number",
                    Optional.empty())));

    @Override
    public BrokerSystem system() {
        return new BrokerSystem(SYSTEM, "Fake", "[1,)", 1, Set.of(), List.of());
    }

    @Override
    public ConnectionSchema schema() {
        return SCHEMA;
    }

    @Override
    public ConnectionReport verify(ClusterSpec cluster, CredentialSource credentials, DriverContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClusterConnection connect(ClusterSpec cluster, CredentialSource credentials, DriverContext context) {
        throw new UnsupportedOperationException();
    }
}
