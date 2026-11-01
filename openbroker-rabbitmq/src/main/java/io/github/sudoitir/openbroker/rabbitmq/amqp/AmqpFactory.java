package io.github.sudoitir.openbroker.rabbitmq.amqp;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultSaslConfig;
import com.rabbitmq.client.SocketConfigurator;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.TlsMaterial;
import io.github.sudoitir.openbroker.rabbitmq.RabbitMqOptions;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster.HostPort;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.net.ssl.SSLSocket;

/**
 * Builds the {@link ConnectionFactory} from the spec, the credentials and the TLS material, and the list of data
 * addresses (cluster endpoints first, then node endpoints).
 *
 * <p>The client's own recovery is off: the connection is reopened by the driver's link supervisor, which asks for
 * credentials and TLS material again. Credentials of the data plane are a user name and password (a bearer token is
 * sent as the password of an empty user name, as RabbitMQ's OAuth 2 plugin expects), or, on {@code amqps} with no
 * credentials, the client certificate of the TLS material (SASL {@code EXTERNAL}).
 */
public final class AmqpFactory {

    private static final int HEARTBEAT_SECONDS = 30;
    private static final int ID_LENGTH = 8;

    private final RabbitMqCluster cluster;
    private final String connectionId = UUID.randomUUID().toString().substring(0, ID_LENGTH);

    public AmqpFactory(RabbitMqCluster cluster) {
        this.cluster = cluster;
    }

    /**
     * A factory for {@code vhost}: recovery off, named {@code openbroker-<connection id>-<vhost>}.
     *
     * @throws io.github.sudoitir.openbroker.BrokerException {@code INVALID_ARGUMENT} when the specification has no
     *     DATA endpoint, mixes {@code amqp} and {@code amqps}, or lacks the credentials or TLS material the endpoints
     *     need
     */
    public ConnectionFactory factory(String vhost) {
        boolean tls = secure(dataEndpoints());
        ConnectionFactory factory = new ConnectionFactory();
        factory.setVirtualHost(vhost);
        factory.setAutomaticRecoveryEnabled(false);
        factory.setTopologyRecoveryEnabled(false);
        factory.setRequestedHeartbeat(HEARTBEAT_SECONDS);
        factory.getClientProperties().put("connection_name", name(vhost));
        Credentials credentials = cluster.credentials().forPlane(Plane.DATA);
        switch (credentials) {
            case Credentials.UsernamePassword account -> {
                factory.setUsername(account.username());
                factory.setPassword(account.password().reveal());
            }
            case Credentials.BearerToken token -> {
                factory.setUsername("");
                factory.setPassword(token.token().reveal());
            }
            case Credentials.None none -> {
                if (!tls) {
                    throw RabbitMqErrors.invalid(
                            "The data plane has no credentials: give a user name and password, or use amqps with a"
                                    + " client certificate.");
                }
                factory.setSaslConfig(DefaultSaslConfig.EXTERNAL);
            }
        }
        if (tls) {
            secure(factory);
        }
        return factory;
    }

    /** The data addresses in the order they are tried: the cluster's endpoints, then each node's. */
    public List<Address> addresses() {
        List<Address> out = new ArrayList<>();
        for (Endpoint endpoint : dataEndpoints()) {
            HostPort hostPort = RabbitMqCluster.dataAddress(endpoint);
            out.add(new Address(unbracket(hostPort.host()), hostPort.port()));
        }
        return List.copyOf(out);
    }

    /** The name of the connection as the broker lists it. */
    String name(String vhost) {
        return "openbroker-" + connectionId + "-" + vhost;
    }

    private List<Endpoint> dataEndpoints() {
        ClusterSpec spec = cluster.spec();
        List<Endpoint> out = new ArrayList<>();
        spec.clusterEndpoints().stream().filter(AmqpFactory::isData).forEach(out::add);
        for (NodeSpec node : spec.nodes()) {
            node.endpoints().stream().filter(AmqpFactory::isData).forEach(out::add);
        }
        if (out.isEmpty()) {
            throw RabbitMqErrors.invalid(
                    "The cluster specification has no DATA endpoint, which AMQP needs: add one such as amqp"
                            + " rabbit.example.com:" + RabbitMqCluster.AMQP_PORT + ".");
        }
        return out;
    }

    private static boolean isData(Endpoint endpoint) {
        return endpoint.plane() == Plane.DATA;
    }

    private static boolean secure(List<Endpoint> endpoints) {
        boolean secure = false;
        boolean plain = false;
        for (Endpoint endpoint : endpoints) {
            if (RabbitMqOptions.AMQPS.equals(endpoint.protocol())) {
                secure = true;
            } else if (RabbitMqOptions.AMQP.equals(endpoint.protocol())) {
                plain = true;
            } else {
                throw RabbitMqErrors.invalid(
                        "A data endpoint's protocol is amqp or amqps, not '" + endpoint.protocol() + "'.");
            }
        }
        if (secure && plain) {
            throw RabbitMqErrors.invalid("The data endpoints mix amqp and amqps: use one of them for all.");
        }
        return secure;
    }

    private void secure(ConnectionFactory factory) {
        TlsMaterial material = cluster.options()
                .tls()
                .current()
                .orElseThrow(
                        () -> RabbitMqErrors.invalid(
                                "A data endpoint uses amqps, and the connection has no TLS material: configure a TlsProvider."));
        if (material.hostnameVerification()) {
            factory.useSslProtocol(material.context());
        } else {
            factory.setSslContextFactory(name -> material.context());
            factory.setSocketFactory(material.context().getSocketFactory());
        }
        if (!material.protocols().isEmpty() || !material.cipherSuites().isEmpty()) {
            SocketConfigurator base = factory.getSocketConfigurator();
            SocketConfigurator limits = socket -> limit(socket, material);
            factory.setSocketConfigurator(base == null ? limits : base.andThen(limits));
        }
    }

    private static void limit(Socket socket, TlsMaterial material) {
        if (socket instanceof SSLSocket ssl) {
            if (!material.protocols().isEmpty()) {
                ssl.setEnabledProtocols(material.protocols().toArray(String[]::new));
            }
            if (!material.cipherSuites().isEmpty()) {
                ssl.setEnabledCipherSuites(material.cipherSuites().toArray(String[]::new));
            }
        }
    }

    private static String unbracket(String host) {
        return host.startsWith("[") && host.endsWith("]") ? host.substring(1, host.length() - 1) : host;
    }
}
