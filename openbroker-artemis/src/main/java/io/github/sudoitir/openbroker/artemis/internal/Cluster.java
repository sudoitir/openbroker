package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Deadline;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.artemis.ArtemisMBeans;
import io.github.sudoitir.openbroker.model.Scope;
import io.github.sudoitir.openbroker.model.Shard;
import io.github.sudoitir.openbroker.spi.DriverContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * What every facet of one connection shares: the nodes, the credentials, the caller's options, and a
 * Jolokia client per node built on the shared transports.
 */
public final class Cluster {

    private static final int MAX_KNOWN_QUEUES = 10_000;

    private final CredentialSource credentials;
    private final DriverContext context;
    private final ObjectMapper mapper = JsonMapper.builder().build();
    private final JolokiaClient.BrokerNames brokerNames;
    private volatile Map<NodeId, NodeHandle> nodes;
    /**
     * A queue's MBean per node, learned with a search and forgotten when the queue is deleted or the
     * broker says it is gone. Bounded, least recently used out first, so a long-lived connection over
     * short-lived queues does not grow without end.
     */
    private final Map<String, String> queueMBeans = Collections.synchronizedMap(new LinkedHashMap<>(256, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
            return size() > MAX_KNOWN_QUEUES;
        }
    });

    public Cluster(ClusterSpec spec, CredentialSource credentials, DriverContext context) {
        this.credentials = credentials;
        this.context = context;
        this.brokerNames = context.shared("artemis.broker-names", NamesHolder.class, NamesHolder::new)
                .names();
        this.nodes = index(spec);
    }

    public void reconfigure(ClusterSpec spec) {
        this.nodes = index(spec);
    }

    public ConnectOptions options() {
        return context.options();
    }

    public DriverContext context() {
        return context;
    }

    public ObjectMapper mapper() {
        return mapper;
    }

    public CredentialSource credentials() {
        return credentials;
    }

    public List<NodeHandle> nodes() {
        return List.copyOf(nodes.values());
    }

    /** The nodes a scope names, in the order the caller gave them. */
    public List<NodeHandle> nodes(Scope scope) {
        return switch (scope) {
            case Scope.Whole w -> nodes();
            case Scope.Shards s -> {
                List<NodeHandle> out = new ArrayList<>();
                for (Shard shard : s.shards()) {
                    if (!(shard instanceof Shard.NodeInstance n)) {
                        throw new BrokerException(
                                Status.INVALID_ARGUMENT,
                                ErrorInfo.of(ErrorReasons.INVALID_ARGUMENT),
                                Dispatch.NOT_SENT,
                                null,
                                "Artemis has no partitions; scope work to node instances.");
                    }
                    out.add(node(n.node()));
                }
                yield out;
            }
        };
    }

    public NodeHandle node(NodeId id) {
        NodeHandle handle = nodes.get(id);
        if (handle == null) {
            throw new BrokerException(
                    Status.NOT_FOUND,
                    ErrorInfo.of(ErrorReasons.INVALID_ARGUMENT),
                    Dispatch.NOT_SENT,
                    id,
                    "This connection has no node " + id + ".");
        }
        return handle;
    }

    /** The node's Jolokia client; a node without a management endpoint cannot be managed. */
    public JolokiaClient jolokia(NodeHandle node) {
        return new JolokiaClient(
                Transports.http(context, context.options().tls().current()),
                node.jolokia()
                        .orElseThrow(() -> new BrokerException(
                                Status.FAILED_PRECONDITION,
                                ErrorInfo.of(ErrorReasons.ENDPOINT_NOT_FOUND),
                                Dispatch.NOT_SENT,
                                node.id(),
                                "This node has no management endpoint.")),
                node.id(),
                () -> credentials.forPlane(Plane.MANAGEMENT),
                mapper,
                brokerNames,
                context.options().interceptor(),
                context.options().clock(),
                context.options().timeouts(),
                context.options().maxResponseBytes());
    }

    /** The node's Jolokia agent, bound to one deadline: the one of the facet call it serves. */
    public NodeJolokia jolokia(NodeHandle node, Deadline deadline) {
        return new NodeJolokia(jolokia(node), () -> deadline);
    }

    /**
     * The MBean of the queue named {@code queue} on this node, whatever its address and routing type.
     *
     * @throws BrokerException with {@code NOT_FOUND} and {@code RESOURCE_GONE} when the node has none
     */
    public String queueMBean(NodeHandle node, String queue, Deadline deadline) {
        String key = node.id().value() + "\u0000" + queue;
        String known = queueMBeans.get(key);
        if (known != null) {
            return known;
        }
        JolokiaClient client = jolokia(node);
        List<String> found = client.search(ArtemisMBeans.queuePattern(client.brokerName(deadline), queue), deadline);
        if (found.isEmpty()) {
            throw new BrokerException(
                    Status.NOT_FOUND,
                    ErrorInfo.of(ErrorReasons.RESOURCE_GONE),
                    Dispatch.NOT_SENT,
                    node.id(),
                    "Queue " + queue + " is not on this node.");
        }
        queueMBeans.put(key, found.getFirst());
        return found.getFirst();
    }

    /** Forgets a queue's MBean after the broker said it is not registered: it was deleted or recreated. */
    public void forgetQueue(NodeHandle node, String queue) {
        queueMBeans.remove(node.id().value() + "\u0000" + queue);
    }

    private static Map<NodeId, NodeHandle> index(ClusterSpec spec) {
        Map<NodeId, NodeHandle> out = new LinkedHashMap<>();
        for (NodeSpec n : spec.nodes()) {
            out.put(n.id(), NodeHandle.of(n));
        }
        return Collections.unmodifiableMap(out);
    }

    /** The resolved broker names, shared across connections. */
    record NamesHolder(JolokiaClient.BrokerNames names) implements AutoCloseable {
        NamesHolder() {
            this(new JolokiaClient.BrokerNames());
        }

        @Override
        public void close() {}
    }
}
