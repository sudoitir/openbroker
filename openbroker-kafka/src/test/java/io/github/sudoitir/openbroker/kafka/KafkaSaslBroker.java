package io.github.sudoitir.openbroker.kafka;

import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Plane;
import java.util.List;
import java.util.Map;
import org.testcontainers.kafka.KafkaContainer;

/**
 * One broker whose client listener asks for SASL PLAIN, for the cases that need a cluster that can
 * refuse a login. Its one account is {@value #USER}.
 */
public final class KafkaSaslBroker {

    public static final String USER = "alice";
    public static final String PASSWORD = "alice-secret";

    private static KafkaContainer broker;

    private KafkaSaslBroker() {}

    /** The broker, started on first use and stopped when the JVM ends. */
    public static synchronized KafkaContainer broker() {
        if (broker == null) {
            KafkaContainer container = new KafkaContainer(KafkaTestCluster.image())
                    .withEnv(
                            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                            "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT")
                    .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
                    .withEnv(
                            "KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG",
                            "org.apache.kafka.common.security.plain.PlainLoginModule required user_" + USER + "=\""
                                    + PASSWORD + "\";")
                    .withEnv("KAFKA_HEAP_OPTS", "-Xms128m -Xmx384m");
            container.start();
            Runtime.getRuntime().addShutdownHook(new Thread(container::stop));
            broker = container;
        }
        return broker;
    }

    /** The cluster, reached over SASL PLAIN. */
    public static ClusterSpec spec() {
        return new ClusterSpec(
                List.of(new NodeSpec(
                        NodeId.of("1"),
                        List.of(Endpoint.of(Plane.DATA, KafkaOptions.PROTOCOL, broker().getBootstrapServers())))),
                List.of(),
                Map.of(
                        KafkaOptions.SECURITY_PROTOCOL, KafkaOptions.SASL_PLAINTEXT,
                        KafkaOptions.SASL_MECHANISM, KafkaOptions.PLAIN));
    }
}
