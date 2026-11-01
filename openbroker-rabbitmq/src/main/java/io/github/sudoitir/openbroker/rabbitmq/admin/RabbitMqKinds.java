package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.model.Kind;
import io.github.sudoitir.openbroker.model.KindId;
import java.util.List;

/** Every {@link KindId} of the driver, its {@code rabbitmq.*} attribute keys and its {@link Kind} declarations. */
public final class RabbitMqKinds {

    public static final KindId VHOST = KindId.of("rabbitmq.vhost");
    public static final KindId EXCHANGE = KindId.of("rabbitmq.exchange");
    public static final KindId CLASSIC_QUEUE = KindId.of("rabbitmq.classic-queue");
    public static final KindId QUORUM_QUEUE = KindId.of("rabbitmq.quorum-queue");
    public static final KindId STREAM = KindId.of("rabbitmq.stream");
    public static final KindId BINDING = KindId.of("rabbitmq.binding");
    public static final KindId EXCHANGE_BINDING = KindId.of("rabbitmq.exchange-binding");
    public static final KindId SHOVEL = KindId.of("rabbitmq.shovel");
    public static final KindId FEDERATION_LINK = KindId.of("rabbitmq.federation-link");
    public static final KindId POLICY = KindId.of("rabbitmq.policy");
    public static final KindId OPERATOR_POLICY = KindId.of("rabbitmq.operator-policy");
    public static final KindId PERMISSION = KindId.of("rabbitmq.permission");
    public static final KindId TOPIC_PERMISSION = KindId.of("rabbitmq.topic-permission");
    public static final KindId USER = KindId.of("rabbitmq.user");
    public static final KindId CONNECTION = KindId.of("rabbitmq.connection");
    public static final KindId CHANNEL = KindId.of("rabbitmq.channel");
    public static final KindId CONSUMER = KindId.of("rabbitmq.consumer");

    private RabbitMqKinds() {}

    /** The declarations of all kinds, in the order above. */
    public static List<Kind> all() {
        throw RabbitMqErrors.unimplemented("RabbitMqKinds.all");
    }
}
