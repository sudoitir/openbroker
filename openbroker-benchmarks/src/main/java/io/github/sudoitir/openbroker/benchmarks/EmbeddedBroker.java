package io.github.sudoitir.openbroker.benchmarks;

import io.github.sudoitir.openbroker.BrokerDrivers;
import io.github.sudoitir.openbroker.ClusterConnection;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.artemis.ArtemisKinds;
import io.github.sudoitir.openbroker.messaging.MessagingObserver;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.List;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;

/**
 * An Artemis server started in this JVM (non-persistent, no security, one netty acceptor on a free
 * loopback port) and a driver connection to its Core acceptor. The connection has no management
 * endpoint: the messaging transport needs Core only, so no console is started.
 */
final class EmbeddedBroker implements AutoCloseable {

    private final ActiveMQServer server;
    private final BrokerDrivers drivers;
    private final ClusterConnection connection;

    EmbeddedBroker(MessagingObserver observer) throws Exception {
        int port = freePort();
        server = ActiveMQServers.newActiveMQServer(
                new ConfigurationImpl()
                        .setPersistenceEnabled(false)
                        .setSecurityEnabled(false)
                        .setJMXManagementEnabled(false)
                        .addAcceptorConfiguration("core", "tcp://127.0.0.1:" + port),
                false);
        server.start();
        drivers = BrokerDrivers.load();
        connection = drivers.connect(new ConnectRequest(
                "artemis",
                ClusterSpec.ofNodes(List.of(new NodeSpec(
                        NodeId.of("primary"), List.of(Endpoint.of(Plane.DATA, "core", "tcp://127.0.0.1:" + port))))),
                CredentialSource.of(Credentials.usernamePassword("bench", "bench")),
                ConnectOptions.defaults().withObserver(observer)));
    }

    Transport transport() {
        return connection.transport().orElseThrow();
    }

    /** A new anycast queue on its own address, as a subscription the transport can send to and process. */
    ResourceId queue(String name) throws Exception {
        server.createQueue(QueueConfiguration.of(name).setAddress(name).setRoutingType(RoutingType.ANYCAST));
        return new ResourceId(ArtemisKinds.QUEUE, ResourcePath.of(name));
    }

    /** Drops what a queue holds, so a send benchmark's backlog does not grow with the run. */
    void purge(String name) throws Exception {
        server.locateQueue(SimpleString.of(name)).deleteAllReferences();
    }

    @Override
    public void close() throws Exception {
        connection.close();
        drivers.close();
        server.stop();
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        }
    }
}
