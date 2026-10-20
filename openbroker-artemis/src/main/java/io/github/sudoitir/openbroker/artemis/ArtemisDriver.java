package io.github.sudoitir.openbroker.artemis;

import io.github.sudoitir.openbroker.BrokerSystem;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectionReport;
import io.github.sudoitir.openbroker.ConnectionSchema;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.artemis.internal.ArtemisConnection;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.spi.BrokerDriver;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.util.List;
import java.util.Set;

/**
 * The Apache ActiveMQ Artemis driver. A node is reached at a {@code MANAGEMENT} endpoint with
 * protocol {@code jolokia} (the console's Jolokia agent, such as {@code
 * http://host:8161/console/jolokia}) and a {@code DATA} endpoint with protocol {@code core} (a Core
 * acceptor, such as {@code tcp://host:61616}).
 */
public final class ArtemisDriver implements BrokerDriver {

    /** The system id, and the prefix of every kind this driver defines. */
    public static final String SYSTEM = "artemis";

    private static final BrokerSystem DESCRIPTION = new BrokerSystem(
            SYSTEM,
            "Apache ActiveMQ Artemis",
            "[2.33.0,)",
            1,
            Set.of("org.apache.activemq.", "org.jolokia."),
            List.of(ArtemisFilters.DIALECT));

    @Override
    public BrokerSystem system() {
        return DESCRIPTION;
    }

    @Override
    public ConnectionSchema schema() {
        return new ConnectionSchema(List.of());
    }

    @Override
    public ConnectionReport verify(ClusterSpec cluster, CredentialSource credentials, DriverContext context) {
        try (ArtemisConnection connection = new ArtemisConnection(DESCRIPTION, cluster, credentials, context)) {
            return new ConnectionReport(
                    connection.nodes().describe(Scope.whole()),
                    connection.access().probe());
        }
    }

    @Override
    public ClusterConnection connect(ClusterSpec cluster, CredentialSource credentials, DriverContext context) {
        return new ArtemisConnection(DESCRIPTION, cluster, credentials, context);
    }
}
