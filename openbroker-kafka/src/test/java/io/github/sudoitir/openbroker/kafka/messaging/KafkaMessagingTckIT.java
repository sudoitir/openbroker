package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.kafka.KafkaDriver;
import io.github.sudoitir.openbroker.messaging.MessageFilter;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import io.github.sudoitir.openbroker.spi.BrokerDriver;
import io.github.sudoitir.openbroker.tck.MessagingTck;
import io.github.sudoitir.openbroker.tck.TckEnvironment;
import java.util.Set;
import org.junit.jupiter.api.Disabled;

/**
 * The messaging TCK against the three-broker cluster, for every kind of subscription the driver
 * declares. A subscription is a group on a topic of one partition (the whole-subscription ordering
 * case needs one) that the case creates through the connection and the environment makes the topic of.
 */
@Disabled(
        "Needs the admin surface of task 2.3: creating a group on a topic, sending and counting through the connection")
class KafkaMessagingTckIT extends MessagingTck {

    @Override
    protected TckEnvironment environment() {
        return new TckEnvironment() {
            @Override
            public BrokerDriver driver() {
                return new KafkaDriver();
            }

            @Override
            public ConnectRequest request() {
                return SharedKafka.request(ConnectOptions.defaults());
            }

            @Override
            public ConnectRequest requestWithWrongPassword(String wrongPassword) {
                return request();
            }

            @Override
            public KindId subscriptionKind() {
                return Subscriptions.GROUP_TOPIC;
            }

            @Override
            public Set<KindId> subscriptionKinds() {
                return Set.of(Subscriptions.GROUP_TOPIC);
            }

            @Override
            public ResourceId subscriptionId(String name) {
                return subscriptionId(subscriptionKind(), name);
            }

            @Override
            public ResourceId subscriptionId(KindId kind, String name) {
                return ResourceId.of(kind.value(), name, name);
            }

            @Override
            public Attributes subscriptionAttributes(String name) {
                return subscriptionAttributes(subscriptionKind(), name);
            }

            @Override
            public Attributes subscriptionAttributes(KindId kind, String name) {
                SharedKafka.create(name, 1);
                return Attributes.builder()
                        .put(StandardAttributes.DESTINATION, SharedKafka.topicId(name))
                        .build();
            }

            @Override
            public MessageFilter nativeEquals(String name, String value) {
                throw new UnsupportedOperationException("Kafka has no server-side message filter");
            }

            @Override
            public String nativeDialect() {
                return "none";
            }
        };
    }
}
