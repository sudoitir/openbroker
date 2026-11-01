package io.github.sudoitir.openbroker.rabbitmq.messaging;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConsumerShutdownSignalCallback;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.rabbitmq.admin.ManagementClient;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.rabbitmq.amqp.AmqpConnections;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import tools.jackson.databind.json.JsonMapper;

/** A cluster, a management client, and an AMQP connection and channel that are mocks, shared by the messaging tests. */
final class MessagingMocks {

    static final ClusterSpec WITH_DATA =
            new ClusterSpec(List.of(), List.of(Endpoint.of(Plane.DATA, "amqp", "localhost:5672")), Map.of());
    static final ClusterSpec WITHOUT_DATA =
            new ClusterSpec(List.of(), List.of(Endpoint.of(Plane.MANAGEMENT, "http", "localhost:15672")), Map.of());

    private static final JsonMapper JSON = JsonMapper.builder().build();

    final RabbitMqCluster cluster = mock(RabbitMqCluster.class);
    final ManagementClient management = mock(ManagementClient.class);
    final AmqpConnections connections = mock(AmqpConnections.class);
    final Connection connection = mock(Connection.class);
    final Channel channel = mock(Channel.class);
    /** The consumer arguments of every {@code basicConsume}, in order. */
    final List<Map<String, Object>> consumed = new ArrayList<>();
    /** The offset a consumer started at a timestamp begins with. */
    long timeStart;

    MessagingMocks(ClusterSpec spec) throws IOException {
        when(cluster.options()).thenReturn(ConnectOptions.defaults());
        when(cluster.spec()).thenReturn(spec);
        when(cluster.management()).thenReturn(management);
        when(cluster.call(any(), any(), any(), any())).thenAnswer(call -> {
            Supplier<?> work = call.getArgument(3);
            return work.get();
        });
        when(connections.connection(any(), any())).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);
        when(channel.isOpen()).thenReturn(true);
    }

    /** Management answers every queue read with {@code json}. */
    void info(String json) {
        when(management.get(anyString(), anyMap(), any(), anyString()))
                .thenReturn(new ManagementClient.Response(JSON.readTree(json), Optional.empty()));
    }

    /** Each {@code basicConsume} delivers the stream messages whose offset is at or after its argument, up to {@code offsets}. */
    void stream(long... offsets) throws IOException {
        when(channel.basicConsume(
                        anyString(),
                        eq(false),
                        anyMap(),
                        any(DeliverCallback.class),
                        any(CancelCallback.class),
                        any(ConsumerShutdownSignalCallback.class)))
                .thenAnswer(call -> {
                    Map<String, Object> arguments = call.getArgument(2);
                    consumed.add(arguments);
                    Object from = arguments.get("x-stream-offset");
                    long start = from instanceof Long number ? number : from instanceof Date ? timeStart : 0;
                    DeliverCallback deliver = call.getArgument(3);
                    for (long offset : offsets) {
                        if (offset >= start) {
                            deliver.handle("tag", delivery(offset, Map.of()));
                        }
                    }
                    return "tag";
                });
    }

    static Delivery delivery(long offset, Map<String, Object> extra) {
        Map<String, Object> headers = new HashMap<>(extra);
        headers.put("x-stream-offset", offset);
        return new Delivery(
                new Envelope(offset + 1, false, "", "events"),
                new AMQP.BasicProperties.Builder().headers(headers).build(),
                ("body-" + offset).getBytes());
    }
}
