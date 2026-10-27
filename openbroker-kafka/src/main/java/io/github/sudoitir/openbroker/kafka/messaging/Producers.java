package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.kafka.admin.KafkaCluster;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.jspecify.annotations.Nullable;

/** Builds the producers the senders and the transactional lanes write through. */
final class Producers {

    /** A transaction that nobody ends is aborted by the broker after this long, which frees its records for readers. */
    private static final int TRANSACTION_TIMEOUT_MS = 30_000;

    private Producers() {}

    /**
     * A producer for a guarantee.
     *
     * <ul>
     *   <li>{@code AT_MOST_ONCE}: {@code acks=1}, no idempotence.
     *   <li>{@code AT_LEAST_ONCE}: {@code acks=all} with idempotence, so the client's own retries
     *       neither duplicate nor reorder.
     *   <li>{@code TRANSACTED}: as at least once, with {@code transactionalId}.
     * </ul>
     */
    static KafkaProducer<byte[], byte[]> create(
            KafkaCluster cluster, Guarantee guarantee, @Nullable String transactionalId) {
        Map<String, Object> config = new HashMap<>(cluster.clientConfig(Plane.DATA));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, (int)
                Math.min(cluster.options().timeouts().call().toMillis(), Integer.MAX_VALUE));
        switch (guarantee) {
            case AT_MOST_ONCE -> {
                config.put(ProducerConfig.ACKS_CONFIG, "1");
                config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
            }
            case AT_LEAST_ONCE -> {
                config.put(ProducerConfig.ACKS_CONFIG, "all");
                config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            }
            case TRANSACTED -> {
                config.put(ProducerConfig.ACKS_CONFIG, "all");
                config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
                config.put(
                        ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                        Objects.requireNonNull(transactionalId, "transactionalId"));
                config.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, TRANSACTION_TIMEOUT_MS);
            }
        }
        return new KafkaProducer<>(config);
    }
}
