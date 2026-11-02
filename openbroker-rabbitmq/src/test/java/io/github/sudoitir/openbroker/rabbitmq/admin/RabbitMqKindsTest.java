package io.github.sudoitir.openbroker.rabbitmq.admin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.model.Kind;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceClass;
import io.github.sudoitir.openbroker.model.Verb;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class RabbitMqKindsTest {

    private static final Map<KindId, Kind> BY_ID =
            RabbitMqKinds.all().stream().collect(Collectors.toMap(Kind::id, Function.identity()));

    private static Kind kind(KindId id) {
        return BY_ID.get(id);
    }

    @Test
    void declaresEverySeventeenKindOnceInOrder() {
        assertThat(RabbitMqKinds.all())
                .extracting(Kind::id)
                .containsExactly(
                        RabbitMqKinds.VHOST,
                        RabbitMqKinds.EXCHANGE,
                        RabbitMqKinds.CLASSIC_QUEUE,
                        RabbitMqKinds.QUORUM_QUEUE,
                        RabbitMqKinds.STREAM,
                        RabbitMqKinds.BINDING,
                        RabbitMqKinds.EXCHANGE_BINDING,
                        RabbitMqKinds.SHOVEL,
                        RabbitMqKinds.FEDERATION_LINK,
                        RabbitMqKinds.POLICY,
                        RabbitMqKinds.OPERATOR_POLICY,
                        RabbitMqKinds.PERMISSION,
                        RabbitMqKinds.TOPIC_PERMISSION,
                        RabbitMqKinds.USER,
                        RabbitMqKinds.CONNECTION,
                        RabbitMqKinds.CHANNEL,
                        RabbitMqKinds.CONSUMER);
        assertThat(RabbitMqKinds.all()).allSatisfy(k -> {
            assertThat(k.id().value()).startsWith("rabbitmq.");
            assertThat(k.supports(Verb.LIST)).isTrue();
            assertThat(k.orderings()).containsExactly("name");
        });
    }

    @Test
    void aStreamCannotBePurgedOrConsumedButCanBeBrowsed() {
        Kind stream = kind(RabbitMqKinds.STREAM);
        assertThat(stream.verbs())
                .containsExactlyInAnyOrder(Verb.LIST, Verb.GET, Verb.CREATE, Verb.DELETE, Verb.BROWSE, Verb.SEND);
        assertThat(stream.supports(Verb.PURGE)).isFalse();
        assertThat(stream.supports(Verb.RECEIVE)).isFalse();
        assertThat(stream.resourceClass()).isEqualTo(ResourceClass.SUBSCRIPTION);
    }

    @Test
    void classicAndQuorumQueuesCannotBeBrowsedOrCounted() {
        for (KindId id : List.of(RabbitMqKinds.CLASSIC_QUEUE, RabbitMqKinds.QUORUM_QUEUE)) {
            Kind queue = kind(id);
            assertThat(queue.verbs())
                    .containsExactlyInAnyOrder(
                            Verb.LIST, Verb.GET, Verb.CREATE, Verb.DELETE, Verb.PURGE, Verb.SEND, Verb.RECEIVE);
            assertThat(queue.supports(Verb.BROWSE)).isFalse();
            assertThat(queue.supports(Verb.COUNT)).isFalse();
        }
    }

    @Test
    void vhostsExchangesAndPolicies() {
        assertThat(kind(RabbitMqKinds.VHOST).resourceClass()).isEqualTo(ResourceClass.NAMESPACE);
        assertThat(kind(RabbitMqKinds.VHOST).verbs())
                .containsExactlyInAnyOrder(Verb.LIST, Verb.GET, Verb.CREATE, Verb.UPDATE, Verb.DELETE);
        assertThat(kind(RabbitMqKinds.VHOST).parent()).isEmpty();
        assertThat(kind(RabbitMqKinds.EXCHANGE).verbs())
                .containsExactlyInAnyOrder(Verb.LIST, Verb.GET, Verb.CREATE, Verb.DELETE, Verb.SEND);
        assertThat(kind(RabbitMqKinds.EXCHANGE).resourceClass()).isEqualTo(ResourceClass.DESTINATION);
        assertThat(kind(RabbitMqKinds.EXCHANGE).parent()).contains(RabbitMqKinds.VHOST);
        for (KindId id : List.of(
                RabbitMqKinds.POLICY,
                RabbitMqKinds.OPERATOR_POLICY,
                RabbitMqKinds.PERMISSION,
                RabbitMqKinds.TOPIC_PERMISSION)) {
            assertThat(kind(id).verbs())
                    .containsExactlyInAnyOrder(Verb.LIST, Verb.GET, Verb.CREATE, Verb.UPDATE, Verb.DELETE);
        }
        assertThat(kind(RabbitMqKinds.POLICY).resourceClass()).isEqualTo(ResourceClass.POLICY);
        assertThat(kind(RabbitMqKinds.PERMISSION).resourceClass()).isEqualTo(ResourceClass.ACCESS_RULE);
    }

    @Test
    void routesAreWiringOrReadOnly() {
        for (KindId id : List.of(RabbitMqKinds.BINDING, RabbitMqKinds.EXCHANGE_BINDING)) {
            assertThat(kind(id).verbs()).containsExactlyInAnyOrder(Verb.LIST, Verb.GET, Verb.CREATE, Verb.DELETE);
            assertThat(kind(id).resourceClass()).isEqualTo(ResourceClass.ROUTE);
        }
        assertThat(kind(RabbitMqKinds.SHOVEL).verbs()).containsExactlyInAnyOrder(Verb.LIST, Verb.GET, Verb.DELETE);
        assertThat(kind(RabbitMqKinds.FEDERATION_LINK).verbs()).containsExactlyInAnyOrder(Verb.LIST, Verb.GET);
    }

    @Test
    void clientsAndUsers() {
        assertThat(kind(RabbitMqKinds.USER).verbs()).containsExactlyInAnyOrder(Verb.LIST, Verb.GET);
        assertThat(kind(RabbitMqKinds.USER).resourceClass()).isEqualTo(ResourceClass.PRINCIPAL);
        assertThat(kind(RabbitMqKinds.CONNECTION).verbs()).containsExactlyInAnyOrder(Verb.LIST, Verb.GET, Verb.CLOSE);
        assertThat(kind(RabbitMqKinds.CHANNEL).verbs()).containsExactlyInAnyOrder(Verb.LIST, Verb.GET);
        assertThat(kind(RabbitMqKinds.CONSUMER).verbs()).isEqualTo(Set.of(Verb.LIST));
        assertThat(kind(RabbitMqKinds.CONSUMER).resourceClass()).isEqualTo(ResourceClass.CONSUMER);
    }

    @Test
    void queueKindsDeclareTheirMetrics() {
        assertThat(kind(RabbitMqKinds.QUORUM_QUEUE).schema().attribute(RabbitMqKinds.DELIVERY_LIMIT))
                .isPresent();
        assertThat(kind(RabbitMqKinds.STREAM).schema().attribute(RabbitMqKinds.COMMITTED_OFFSET))
                .isPresent();
        assertThat(kind(RabbitMqKinds.CLASSIC_QUEUE).schema().attribute("openbroker.backlog"))
                .isPresent();
    }
}
