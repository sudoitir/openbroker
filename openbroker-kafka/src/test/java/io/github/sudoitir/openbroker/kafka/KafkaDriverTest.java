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
    void theDriverSupportsKafka30AndLater() {
        assertThat(new KafkaDriver().system().supportedVersions()).isEqualTo("[3.0.0,)");
    }

    @Test
    void theSchemaDescribesTheSecurityProtocolTheMechanismTheClientIdAndTheWatchInterval() {
        assertThat(new KafkaDriver().schema().options())
                .extracting(OptionSpec::key)
                .containsExactly("security.protocol", "sasl.mechanism", "client.id", "watch.interval");
        assertThat(new KafkaDriver()
                        .schema()
                        .option("security.protocol")
                        .orElseThrow()
                        .allowedValues())
                .containsExactly("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL");
    }

    @Test
    void noOptionIsSensitive() {
        assertThat(new KafkaDriver().schema().options()).noneMatch(OptionSpec::sensitive);
    }
}
