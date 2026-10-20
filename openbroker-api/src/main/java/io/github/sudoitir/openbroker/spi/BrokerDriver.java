package io.github.sudoitir.openbroker.spi;

import io.github.sudoitir.openbroker.BrokerSystem;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectionReport;
import io.github.sudoitir.openbroker.ConnectionSchema;
import io.github.sudoitir.openbroker.CredentialSource;

/**
 * A driver for one system. Found through {@link java.util.ServiceLoader} on the SDK's own class
 * loader, or given to {@code BrokerDrivers.of}. Must have a public no-argument constructor when
 * loaded as a service, and must be thread-safe.
 */
public interface BrokerDriver {

    BrokerSystem system();

    ConnectionSchema schema();

    /** Reaches the cluster once, reports what it found, and keeps nothing open. */
    ConnectionReport verify(ClusterSpec cluster, CredentialSource credentials, DriverContext context);

    ClusterConnection connect(ClusterSpec cluster, CredentialSource credentials, DriverContext context);
}
