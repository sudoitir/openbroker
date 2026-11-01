package io.github.sudoitir.openbroker.rabbitmq.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownSignalException;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.notification.BrokerEvent;
import io.github.sudoitir.openbroker.notification.Gap;
import io.github.sudoitir.openbroker.notification.NotificationListener;
import io.github.sudoitir.openbroker.notification.StreamState;
import io.github.sudoitir.openbroker.rabbitmq.amqp.AmqpConnections;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RabbitMqNotificationsTest {

    private static final Map<String, String> JSON = Map.of("Content-Type", "application/json");
    private static final NotificationListener LISTENER = new NotificationListener() {
        @Override
        public void onEvent(BrokerEvent event) {
            // Not what these cases are about.
        }

        @Override
        public void onGap(Gap gap) {
            // Not what these cases are about.
        }
    };

    private FakeManagement server;
    private AmqpConnections connections;
    private Channel channel;
    private RabbitMqNotifications notifications;

    @BeforeEach
    void start() throws Exception {
        server = new FakeManagement();
        server.respond("GET", "/api/consumers", 200, JSON, "[]");
        connections = mock(AmqpConnections.class);
        Connection connection = mock(Connection.class);
        channel = mock(Channel.class);
        AMQP.Queue.DeclareOk declared = mock(AMQP.Queue.DeclareOk.class);
        when(declared.getQueue()).thenReturn("amq.gen-1");
        when(connections.connection(anyString(), any(Deadline.class))).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);
        when(channel.queueDeclare("", false, true, true, null)).thenReturn(declared);
    }

    @AfterEach
    void stop() {
        if (notifications != null) {
            notifications.close();
        }
        server.close();
    }

    private RabbitMqNotifications notifications(Optional<AmqpConnections> amqp) {
        RabbitMqCluster cluster = new RabbitMqCluster(
                new ClusterSpec(
                        List.of(), List.of(server.endpoint(), Endpoint.of(Plane.DATA, "amqp", "localhost")), Map.of()),
                CredentialSource.of(Credentials.usernamePassword("ops", "s3cret")),
                new FakeManagement.TestContext(ConnectOptions.defaults()));
        notifications = new RabbitMqNotifications(cluster, mock(RabbitMqResources.class), amqp);
        return notifications;
    }

    private void plugins(String... enabled) {
        String list = String.join("\",\"", enabled);
        server.respond(
                "GET",
                "/api/nodes",
                200,
                JSON,
                "[{\"name\":\"rabbit@a\",\"running\":true,\"enabled_plugins\":[\"" + list + "\"]}]");
    }

    @Test
    void readsTheEventExchangeWhenThePluginIsEnabledAndTheAccountMayBind() throws Exception {
        plugins("rabbitmq_management", "rabbitmq_event_exchange");
        RabbitMqNotifications n = notifications(Optional.of(connections));
        AutoCloseable subscription = n.subscribe(Scope.whole(), LISTENER);
        verify(channel).queueBind("amq.gen-1", "amq.rabbitmq.event", "#");
        assertThat(n.state()).containsOnlyKeys(RabbitMqCluster.CLUSTER);
        assertThat(n.state().get(RabbitMqCluster.CLUSTER).phase()).isEqualTo(StreamState.Phase.ACTIVE);
        subscription.close();
        verify(channel).close();
        assertThat(n.state()).isEmpty();
    }

    @Test
    void pollsWhenThePluginIsNotEnabled() throws Exception {
        plugins("rabbitmq_management");
        RabbitMqNotifications n = notifications(Optional.of(connections));
        AutoCloseable subscription = n.subscribe(Scope.whole(), LISTENER);
        verify(channel, never()).queueBind(anyString(), anyString(), anyString());
        subscription.close();
    }

    @Test
    void pollsWhenThereIsNoDataEndpoint() throws Exception {
        RabbitMqNotifications n = notifications(Optional.empty());
        AutoCloseable subscription = n.subscribe(Scope.whole(), LISTENER);
        assertThat(server.received().stream().filter(r -> r.path().equals("/api/nodes")))
                .isEmpty();
        subscription.close();
    }

    @Test
    void fallsBackToPollingWhenTheBindingIsRefused() throws Exception {
        plugins("rabbitmq_management", "rabbitmq_event_exchange");
        ShutdownSignalException refused = new ShutdownSignalException(
                false,
                false,
                new AMQP.Channel.Close.Builder()
                        .replyCode(403)
                        .replyText("ACCESS_REFUSED")
                        .build(),
                channel);
        doThrow(new IOException(refused)).when(channel).queueBind(anyString(), anyString(), anyString());
        RabbitMqNotifications n = notifications(Optional.of(connections));
        AutoCloseable subscription = n.subscribe(Scope.whole(), LISTENER);
        subscription.close();
    }

    @Test
    void pollsWhenThePluginsCannotBeRead() throws Exception {
        server.respond("GET", "/api/nodes", 500, JSON, "{}");
        RabbitMqNotifications n = notifications(Optional.of(connections));
        AutoCloseable subscription = n.subscribe(Scope.whole(), LISTENER);
        subscription.close();
        verify(channel, never()).queueBind(anyString(), anyString(), anyString());
    }

    @Test
    void theTwoArgumentFormPolls() throws Exception {
        RabbitMqCluster cluster = FakeManagement.cluster(server.endpoint());
        notifications = new RabbitMqNotifications(cluster, mock(RabbitMqResources.class));
        notifications.subscribe(Scope.whole(), LISTENER).close();
        assertThat(notifications.state()).isEmpty();
    }

    @Test
    void resetClosesTheOpenSubscriptions() throws Exception {
        plugins("rabbitmq_management", "rabbitmq_event_exchange");
        RabbitMqNotifications n = notifications(Optional.of(connections));
        n.subscribe(Scope.whole(), LISTENER);
        n.reset();
        verify(channel).close();
        assertThat(n.state()).isEmpty();
    }
}
