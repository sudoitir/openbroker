package io.github.sudoitir.openbroker.kafka.schema;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.kafka.KafkaDriver;
import io.github.sudoitir.openbroker.kafka.KafkaOptions;
import io.github.sudoitir.openbroker.kafka.KafkaTestCluster;
import io.github.sudoitir.openbroker.messaging.Cursor;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/** One Apicurio registry beside the shared Kafka cluster, for the schema catalog tests. */
final class SchemaRegistrySupport {

    static final String IMAGE = "apicurio/apicurio-registry:3.3.5";

    private static GenericContainer<?> registry;
    private static BrokerDrivers drivers;

    private SchemaRegistrySupport() {}

    static synchronized void start() {
        KafkaTestCluster.shared();
        if (registry == null) {
            registry = new GenericContainer<>(IMAGE)
                    .withExposedPorts(8080)
                    .waitingFor(Wait.forHttp("/apis/ccompat/v7/subjects")
                            .forPort(8080)
                            .forStatusCode(200))
                    .withStartupTimeout(Duration.ofMinutes(4));
            registry.start();
        }
        if (drivers == null) {
            drivers = BrokerDrivers.load();
        }
    }

    static String url() {
        return "http://" + registry.getHost() + ":" + registry.getMappedPort(8080) + "/apis/ccompat/v7";
    }

    static ClusterConnection connect(Map<String, String> options) {
        return drivers.connect(new ConnectRequest(
                KafkaDriver.SYSTEM,
                KafkaTestCluster.shared().spec().withOptions(options),
                CredentialSource.none(),
                ConnectOptions.defaults()));
    }

    static Map<String, String> confluent(String credentials) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put(KafkaOptions.REGISTRY_KIND, KafkaOptions.REGISTRY_CONFLUENT);
        options.put(KafkaOptions.REGISTRY_URL, url());
        if (credentials != null) {
            options.put(KafkaOptions.REGISTRY_CREDENTIALS, credentials);
        }
        return options;
    }

    static String subject() {
        return "ob" + UUID.randomUUID().toString().replace("-", "");
    }

    static List<Integer> versions(SchemaCatalog catalog, String subject) {
        try {
            return catalog.subject(subject).versions().stream()
                    .map(SubjectVersion::version)
                    .toList();
        } catch (BrokerException e) {
            if (e.status() == Status.NOT_FOUND) {
                return List.of();
            }
            throw e;
        }
    }

    static Subject find(SchemaCatalog catalog, String name) {
        Cursor cursor = new Cursor("");
        while (true) {
            Subjects page = catalog.subjects(cursor);
            for (Subject subject : page.items()) {
                if (subject.name().equals(name)) {
                    return subject;
                }
            }
            if (page.next().isEmpty()) {
                return null;
            }
            cursor = page.next().get();
        }
    }
}
