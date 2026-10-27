package io.github.sudoitir.openbroker.kafka.messaging;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.kafka.KafkaOptions;
import io.github.sudoitir.openbroker.kafka.KafkaTestCluster;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * One Kafka broker that wants a login: SASL over plain TCP with the {@code PLAIN} and {@code
 * SCRAM-SHA-256} mechanisms and an authorizer. {@code admin} is a super user; {@code app} may use
 * topics {@code t.*} and groups {@code g.*}; {@code limited} may use only topics {@code t.ok.*}. A
 * connected client is made to authenticate again every few seconds, so a rotated password is
 * noticed without a restart.
 */
final class SaslKafka implements AutoCloseable {

    static final String ADMIN = "admin";
    static final String ADMIN_PASSWORD = "admin-secret";
    static final String APP = "app";
    static final String APP_PASSWORD = "app-secret";
    static final String LIMITED = "limited";
    static final String LIMITED_PASSWORD = "limited-secret";
    static final String LOCKED = "locked";
    static final String LOCKED_PASSWORD = "locked-secret";

    /** How often a connected client must authenticate again. */
    static final int REAUTH_MS = 2_000;

    private final GenericContainer<?> container;
    private final int port;

    private SaslKafka() {
        this.port = freePort();
        String jaas = "org.apache.kafka.common.security.plain.PlainLoginModule required user_" + ADMIN + "=\""
                + ADMIN_PASSWORD + "\" user_" + APP + "=\"" + APP_PASSWORD + "\" user_" + LIMITED + "=\""
                + LIMITED_PASSWORD + "\" user_" + LOCKED + "=\"" + LOCKED_PASSWORD + "\";";
        this.container = new GenericContainer<>(
                        System.getProperty(KafkaTestCluster.IMAGE_PROPERTY, KafkaTestCluster.DEFAULT_IMAGE))
                .withEnv(Map.ofEntries(
                        Map.entry("KAFKA_NODE_ID", "1"),
                        Map.entry("KAFKA_PROCESS_ROLES", "broker,controller"),
                        Map.entry(
                                "KAFKA_LISTENERS",
                                "SASL://0.0.0.0:" + port + ",BROKER://0.0.0.0:9094,CONTROLLER://0.0.0.0:9093"),
                        Map.entry(
                                "KAFKA_ADVERTISED_LISTENERS", "SASL://localhost:" + port + ",BROKER://localhost:9094"),
                        Map.entry(
                                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                                "SASL:SASL_PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT"),
                        Map.entry("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER"),
                        Map.entry("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER"),
                        Map.entry("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093"),
                        Map.entry("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN,SCRAM-SHA-256"),
                        Map.entry("KAFKA_LISTENER_NAME_SASL_PLAIN_SASL_JAAS_CONFIG", jaas),
                        Map.entry(
                                "KAFKA_LISTENER_NAME_SASL_SCRAM-SHA-256_SASL_JAAS_CONFIG",
                                "org.apache.kafka.common.security.scram.ScramLoginModule required;"),
                        Map.entry(
                                "KAFKA_AUTHORIZER_CLASS_NAME",
                                "org.apache.kafka.metadata.authorizer.StandardAuthorizer"),
                        // The broker and controller talk to each other in the clear, as the anonymous principal.
                        Map.entry("KAFKA_SUPER_USERS", "User:" + ADMIN + ";User:ANONYMOUS"),
                        Map.entry("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "false"),
                        Map.entry("KAFKA_CONNECTIONS_MAX_REAUTH_MS", String.valueOf(REAUTH_MS)),
                        Map.entry("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1"),
                        Map.entry("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1"),
                        Map.entry("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1"),
                        Map.entry("KAFKA_DEFAULT_REPLICATION_FACTOR", "1"),
                        Map.entry("KAFKA_GROUP_CONSUMER_MIN_HEARTBEAT_INTERVAL_MS", "250"),
                        Map.entry("KAFKA_GROUP_CONSUMER_HEARTBEAT_INTERVAL_MS", "500"),
                        Map.entry("KAFKA_HEAP_OPTS", "-Xms128m -Xmx384m")))
                .withExposedPorts(port)
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                        .withPortBindings(new PortBinding(Ports.Binding.bindPort(port), new ExposedPort(port))))
                .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1));
    }

    static SaslKafka start() {
        SaslKafka kafka = new SaslKafka();
        try {
            kafka.container.start();
            kafka.prepare();
        } catch (RuntimeException e) {
            kafka.close();
            throw e;
        }
        return kafka;
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("No free port", e);
        }
    }

    String bootstrap() {
        return "localhost:" + port;
    }

    ClusterSpec spec(String mechanism) {
        return new ClusterSpec(
                List.of(),
                List.of(Endpoint.of(Plane.DATA, KafkaOptions.PROTOCOL, bootstrap())),
                Map.of(
                        KafkaOptions.SECURITY_PROTOCOL,
                        KafkaOptions.SASL_PLAINTEXT,
                        KafkaOptions.SASL_MECHANISM,
                        mechanism));
    }

    /** An Admin client logged in as the super user. */
    Admin admin() {
        return Admin.create(Map.of(
                "bootstrap.servers",
                bootstrap(),
                "security.protocol",
                "SASL_PLAINTEXT",
                "sasl.mechanism",
                "PLAIN",
                "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + ADMIN
                        + "\" password=\"" + ADMIN_PASSWORD + "\";"));
    }

    /** Sets the password {@code user} logs in with under SCRAM-SHA-256. */
    void scramPassword(String user, String password) {
        try (Admin admin = admin()) {
            admin.alterUserScramCredentials(List.of(new UserScramCredentialUpsertion(
                            user, new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 4096), password)))
                    .all()
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException("Could not set the password of " + user, e);
        }
    }

    /** Lets {@code user} use topics {@code t.*} and groups {@code g.*}. */
    void grant(String user) {
        try (Admin admin = admin()) {
            admin.createAcls(List.of(
                            allow(user, ResourceType.TOPIC, "t.", PatternType.PREFIXED),
                            allow(user, ResourceType.GROUP, "g.", PatternType.PREFIXED)))
                    .all()
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException("Could not grant " + user, e);
        }
    }

    /** Lets {@code user} use exactly the topic {@code topic}, and groups {@code g.*}. */
    void grantOnly(String user, String topic) {
        try (Admin admin = admin()) {
            admin.createAcls(List.of(
                            allow(user, ResourceType.TOPIC, topic, PatternType.LITERAL),
                            allow(user, ResourceType.GROUP, "g.", PatternType.PREFIXED)))
                    .all()
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException("Could not grant " + user, e);
        }
    }

    private void prepare() {
        try (Admin admin = admin()) {
            admin.createAcls(List.of(
                            allow(APP, ResourceType.TOPIC, "t.", PatternType.PREFIXED),
                            allow(APP, ResourceType.GROUP, "g.", PatternType.PREFIXED),
                            allow(APP, ResourceType.TRANSACTIONAL_ID, "openbroker-", PatternType.PREFIXED),
                            allow(LIMITED, ResourceType.TOPIC, "t.ok.", PatternType.PREFIXED),
                            allow(LIMITED, ResourceType.GROUP, "g.", PatternType.PREFIXED)))
                    .all()
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException("Could not set up the accounts", e);
        }
        scramPassword(APP, APP_PASSWORD);
    }

    private static AclBinding allow(String user, ResourceType type, String name, PatternType pattern) {
        return new AclBinding(
                new ResourcePattern(type, name, pattern),
                new AccessControlEntry("User:" + user, "*", AclOperation.ALL, AclPermissionType.ALLOW));
    }

    @Override
    public void close() {
        container.stop();
    }
}
