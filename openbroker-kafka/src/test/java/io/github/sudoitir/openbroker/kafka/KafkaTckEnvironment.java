package io.github.sudoitir.openbroker.kafka;

import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.kafka.admin.KafkaKinds;
import io.github.sudoitir.openbroker.messaging.MessageFilter;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.PartitionId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.Shard;
import io.github.sudoitir.openbroker.spi.BrokerDriver;
import io.github.sudoitir.openbroker.tck.TckEnvironment;
import java.util.function.Supplier;

/**
 * What the TCKs ask of the Kafka driver: a subscription is a consumer group on a topic; making one
 * makes the topic and starts the group at the beginning of it. The transport cases are skipped until
 * the connection offers a transport. The cluster is the caller's.
 */
final class KafkaTckEnvironment implements TckEnvironment {

    private final Supplier<ConnectRequest> request;

    KafkaTckEnvironment(Supplier<ConnectRequest> request) {
        this.request = request;
    }

    @Override
    public BrokerDriver driver() {
        return new KafkaDriver();
    }

    @Override
    public ConnectRequest request() {
        return request.get();
    }

    @Override
    public ConnectRequest requestWithWrongPassword(String wrongPassword) {
        return new ConnectRequest(
                KafkaDriver.SYSTEM,
                KafkaSaslBroker.spec(),
                CredentialSource.of(Credentials.usernamePassword(KafkaSaslBroker.USER, wrongPassword)),
                ConnectOptions.defaults());
    }

    @Override
    public KindId subscriptionKind() {
        return KafkaKinds.GROUP_TOPIC;
    }

    @Override
    public ResourceId subscriptionId(String name) {
        return new ResourceId(KafkaKinds.GROUP_TOPIC, ResourcePath.of(name + "-group", name));
    }

    @Override
    public Attributes subscriptionAttributes(String name) {
        return Attributes.empty();
    }

    @Override
    public Shard browseShard(ResourceId subscription) {
        return Shard.partition(PartitionId.of("0"));
    }

    @Override
    public MessageFilter nativeEquals(String name, String value) {
        return new MessageFilter(nativeDialect(), name + "=" + value);
    }

    @Override
    public String nativeDialect() {
        // Kafka filters nowhere, so no case that needs a native filter runs.
        return "kafka.none";
    }
}
