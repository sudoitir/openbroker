package io.github.sudoitir.openbroker.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.OptionSpec;
import io.github.sudoitir.openbroker.spi.BrokerDriver;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;

class KafkaDriverTest {

    @Test
    void theDriverIsRegisteredThroughTheServiceLoader() {
        assertThat(ServiceLoader.load(BrokerDriver.class))
                .anyMatch(d -> d instanceof KafkaDriver && d.system().id().equals("kafka"));
    }

    @Test
    void theDriverSupportsKafka28AndLater() {
        assertThat(new KafkaDriver().system().supportedVersions()).isEqualTo("[2.8.0,)");
    }

    @Test
    void theSchemaDescribesTheConnectionAndTheSchemaRegistry() {
        assertThat(new KafkaDriver().schema().options())
                .extracting(OptionSpec::key)
                .containsExactly(
                        "security.protocol",
                        "sasl.mechanism",
                        "client.id",
                        "watch.interval",
                        "setup.grace",
                        "registry.kind",
                        "registry.url",
                        "registry.credentials");
        assertThat(new KafkaDriver()
                        .schema()
                        .option("security.protocol")
                        .orElseThrow()
                        .allowedValues())
                .containsExactly("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL");
    }

    /** The registry credential is the one sensitive option: it is never logged, shown or returned. */
    @Test
    void onlyTheRegistryCredentialIsSensitive() {
        assertThat(new KafkaDriver().schema().options())
                .filteredOn(OptionSpec::sensitive)
                .extracting(OptionSpec::key)
                .containsExactly("registry.credentials");
    }
}
