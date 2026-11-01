package io.github.sudoitir.openbroker.rabbitmq;

import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.messaging.MessageFilter;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.PartitionId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.Shard;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import io.github.sudoitir.openbroker.spi.BrokerDriver;
import io.github.sudoitir.openbroker.tck.TckEnvironment;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * What the TCKs ask of the RabbitMQ driver: a subscription is a durable queue (of one kind) in the default virtual
 * host, and a message sent to it is sent through the default exchange. The cluster is the shared three-node test
 * cluster, whose administrator may do everything.
 */
public final class RabbitMqTckEnvironment implements TckEnvironment {

    private static final String VHOST = "/";

    private final KindId kind;

    /** An environment whose subscriptions are queues or streams of {@code kind}. */
    public RabbitMqTckEnvironment(KindId kind) {
        this.kind = kind;
    }

    @Override
    public BrokerDriver driver() {
        return new RabbitMqDriver();
    }

    @Override
    public ConnectRequest request() {
        RabbitMqTestCluster cluster = RabbitMqTestCluster.shared();
        return new ConnectRequest(
                RabbitMqDriver.SYSTEM,
                cluster.spec(),
                CredentialSource.of(cluster.credentials()),
                ConnectOptions.defaults());
    }

    @Override
    public ConnectRequest requestWithWrongPassword(String wrongPassword) {
        return new ConnectRequest(
                RabbitMqDriver.SYSTEM,
                RabbitMqTestCluster.shared().spec(),
                CredentialSource.of(Credentials.usernamePassword(RabbitMqTestCluster.ADMIN_USER, wrongPassword)),
                ConnectOptions.defaults());
    }

    @Override
    public Optional<ConnectRequest> requestToAddress(String host, int port) {
        return Optional.of(new ConnectRequest(
                RabbitMqDriver.SYSTEM,
                new ClusterSpec(
                        List.of(),
                        List.of(
                                Endpoint.of(Plane.MANAGEMENT, RabbitMqOptions.HTTP, "http://" + host + ":" + port),
                                Endpoint.of(Plane.DATA, RabbitMqOptions.AMQP, host + ":" + port)),
                        Map.of()),
                CredentialSource.of(RabbitMqTestCluster.shared().credentials()),
                ConnectOptions.defaults()));
    }

    @Override
    public KindId subscriptionKind() {
        return kind;
    }

    @Override
    public ResourceId subscriptionId(String name) {
        return new ResourceId(kind, ResourcePath.of(VHOST, name));
    }

    @Override
    public Attributes subscriptionAttributes(String name) {
        return Attributes.builder().put(StandardAttributes.DURABLE, true).build();
    }

    @Override
    public Shard browseShard(ResourceId subscription) {
        // A stream is read as one partition 0; a queue is not browsed, so its shard is never used.
        return Shard.partition(PartitionId.of("0"));
    }

    @Override
    public MessageFilter nativeEquals(String name, String value) {
        return new MessageFilter(nativeDialect(), name + "=" + value);
    }

    @Override
    public String nativeDialect() {
        // RabbitMQ declares no filter dialect, so no case that needs a native filter runs.
        return "rabbitmq.none";
    }
}
