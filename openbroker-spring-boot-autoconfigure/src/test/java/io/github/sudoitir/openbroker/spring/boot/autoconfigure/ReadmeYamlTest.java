package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.artemis.ArtemisDriver;
import io.github.sudoitir.openbroker.kafka.KafkaDriver;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.mock.env.MockEnvironment;

/** The YAML in the README's Spring Boot section is the YAML that binds, against the real drivers' schemas. */
class ReadmeYamlTest {

    private static String readmeYaml() throws IOException {
        String readme = Files.readString(Path.of("..", "README.md"));
        String section = readme.substring(readme.indexOf("## Spring Boot"), readme.indexOf("## Writing a driver"));
        Matcher yaml = Pattern.compile("```yaml\\n(.*?)```", Pattern.DOTALL).matcher(section);
        assertThat(yaml.find()).as("a yaml block in the Spring Boot section").isTrue();
        return yaml.group(1);
    }

    @Test
    void theReadmesConfigurationBindsAndPassesTheDriversSchemas() throws IOException {
        MockEnvironment environment = Yaml.environment(readmeYaml());
        environment.setProperty("ORDERS_PASSWORD", TestSecrets.PASSWORD);
        environment.setProperty("BILLING_PASSWORD", TestSecrets.OTHER_PASSWORD);
        BrokerDrivers drivers = BrokerDrivers.of(List.of(new KafkaDriver(), new ArtemisDriver()));
        ConnectionBinder binder = new ConnectionBinder(environment, drivers);

        Connection orders = binder.bind("orders", null);
        Connection billing = binder.bind("billing", null);

        assertThat(orders.system()).isEqualTo("kafka");
        assertThat(orders.cluster().clusterEndpoints()).hasSize(2);
        assertThat(orders.credentials().toCredentials("orders", "credentials"))
                .isEqualTo(Credentials.usernamePassword("orders-app", TestSecrets.PASSWORD));
        assertThat(orders.options().sslBundle()).isEqualTo("kafka");
        assertThat(orders.sender().guarantee()).isEqualTo(Guarantee.AT_LEAST_ONCE);
        assertThat(orders.processor().concurrency()).isEqualTo(4);
        assertThat(billing.system()).isEqualTo("artemis");
        assertThat(billing.cluster().nodes().getFirst().endpoint(Plane.MANAGEMENT, "jolokia"))
                .isPresent();
        assertThat(Binder.get(environment)
                        .bind("openbroker", OpenBrokerProperties.class)
                        .get()
                        .client()
                        .connectTimeout())
                .isEqualTo(Duration.ofSeconds(5));
    }
}
