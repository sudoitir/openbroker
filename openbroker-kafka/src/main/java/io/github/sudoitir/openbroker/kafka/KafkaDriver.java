package io.github.sudoitir.openbroker.kafka;

import io.github.sudoitir.openbroker.BrokerSystem;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectionReport;
import io.github.sudoitir.openbroker.ConnectionSchema;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.kafka.admin.KafkaConnection;
import io.github.sudoitir.openbroker.spi.BrokerDriver;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.util.List;
import java.util.Set;

/**
 * The Apache Kafka driver, for Kafka 3.0 and later, in ZooKeeper and KRaft mode. The cluster is reached at its bootstrap
 * servers: the cluster endpoints on the {@code DATA} plane with protocol {@code kafka}, such as
 * {@code broker-1:9092}. Its brokers are the nodes.
 */
public final class KafkaDriver implements BrokerDriver {

    /** The system id, and the prefix of every kind this driver defines. */
    public static final String SYSTEM = "kafka";

    private static final BrokerSystem DESCRIPTION =
            new BrokerSystem(SYSTEM, "Apache Kafka", "[3.0.0,)", 1, Set.of("org.apache.kafka."), List.of());

    @Override
    public BrokerSystem system() {
        return DESCRIPTION;
    }

    @Override
    public ConnectionSchema schema() {
        return KafkaOptions.SCHEMA;
    }

    @Override
    public ConnectionReport verify(ClusterSpec cluster, CredentialSource credentials, DriverContext context) {
        try (KafkaConnection connection = new KafkaConnection(DESCRIPTION, cluster, credentials, context)) {
            return connection.verify();
        }
    }

    @Override
    public ClusterConnection connect(ClusterSpec cluster, CredentialSource credentials, DriverContext context) {
        return new KafkaConnection(DESCRIPTION, cluster, credentials, context);
    }
}
