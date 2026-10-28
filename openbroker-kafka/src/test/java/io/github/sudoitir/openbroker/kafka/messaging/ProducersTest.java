package io.github.sudoitir.openbroker.kafka.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.messaging.Guarantee;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

class ProducersTest {

    private static Map<String, Object> configFor(Guarantee guarantee) {
        Map<String, Object> config = new HashMap<>();
        Producers.guarantee(config, guarantee, "tx-1");
        return config;
    }

    @Test
    void atMostOnceNeverWritesABatchAgainBecauseTheClientDoesNotRetry() {
        Map<String, Object> config = configFor(Guarantee.AT_MOST_ONCE);

        assertThat(config).containsEntry(ProducerConfig.RETRIES_CONFIG, 0);
        assertThat(config).containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        assertThat(config).containsEntry(ProducerConfig.ACKS_CONFIG, "1");
    }

    @Test
    void atLeastOnceAndTransactedRetryThroughAnIdempotentProducer() {
        assertThat(configFor(Guarantee.AT_LEAST_ONCE))
                .containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
                .doesNotContainKey(ProducerConfig.RETRIES_CONFIG);
        assertThat(configFor(Guarantee.TRANSACTED)).containsEntry(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-1");
    }
}
