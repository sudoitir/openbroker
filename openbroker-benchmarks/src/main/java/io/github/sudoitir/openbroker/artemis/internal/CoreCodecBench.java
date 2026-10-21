package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.artemis.ArtemisKinds;
import io.github.sudoitir.openbroker.messaging.Blob;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.core.client.impl.ClientMessageImpl;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * {@code CoreBodies.write} and {@code read} of a 1 KiB message, with no broker. {@code CoreBodies}
 * is package-private, so this class sits in its package (the classpath allows it); the session
 * only makes messages, which a stand-in does.
 */
@State(org.openjdk.jmh.annotations.Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class CoreCodecBench {

    private static final ResourceId QUEUE = new ResourceId(ArtemisKinds.QUEUE, ResourcePath.of("codec"));
    private static final NodeId NODE = NodeId.of("primary");

    private ClientSession session;
    private Clock clock;
    private OutboundMessage outbound;
    private ClientMessage encoded;

    @Setup
    public void setUp() throws Exception {
        session = (ClientSession) Proxy.newProxyInstance(
                ClientSession.class.getClassLoader(), new Class<?>[] {ClientSession.class}, (proxy, method, args) -> {
                    if (method.getName().equals("createMessage") && args.length == 5) {
                        return new ClientMessageImpl(
                                (byte) args[0], (boolean) args[1], (long) args[2], (long) args[3], (byte) args[4], 512);
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        clock = Clock.systemUTC();
        outbound = new OutboundMessage(
                MessageHeaders.builder().groupId("group").type("order.created").build(),
                Map.of("tenant", PropertyValue.of("acme"), "attempt", PropertyValue.of(3L)),
                MessageBody.of(Blob.of(new byte[1024])),
                Map.of());
        encoded = CoreBodies.write(session, outbound, clock, Optional.empty()).message();
    }

    @Benchmark
    public ClientMessage write() throws Exception {
        return CoreBodies.write(session, outbound, clock, Optional.empty()).message();
    }

    @Benchmark
    public Message read() throws Exception {
        return CoreBodies.read(encoded, QUEUE, NODE, 1L << 20, Optional.empty());
    }
}
