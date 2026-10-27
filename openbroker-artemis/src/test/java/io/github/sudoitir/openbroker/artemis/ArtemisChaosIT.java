package io.github.sudoitir.openbroker.artemis;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.NodeSpec;
import io.github.sudoitir.openbroker.Timeouts;
import io.github.sudoitir.openbroker.messaging.MessageFilter;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import io.github.sudoitir.openbroker.spi.BrokerDriver;
import io.github.sudoitir.openbroker.tck.ChaosTck;
import io.github.sudoitir.openbroker.tck.TckChaos;
import io.github.sudoitir.openbroker.tck.TckEnvironment;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.Network;
import org.testcontainers.toxiproxy.ToxiproxyContainer;

/**
 * The chaos TCK against Artemis: one broker for the kill and partition cases, and a shared-store
 * primary and backup, on a Docker volume that holds their journal, for the failover cases. Every
 * broker sits behind Toxiproxy on its Core port; the proxy port outlives a kill and start of the
 * broker, and a cut holds the traffic. The image is the build's {@code artemis.image}.
 *
 * <p>A pair that a case has disturbed is replaced by a fresh one for the next case that uses it.
 */
class ArtemisChaosIT extends ChaosTck {

    private static final NodeId SINGLE = NodeId.of("single");
    private static final NodeId PRIMARY = NodeId.of("primary");
    private static final NodeId BACKUP = NodeId.of("backup");
    private static final int FIRST_PROXY_PORT = 8666;
    private static final int PROXY_PORTS = 32;
    private static final int CORE = 61616;

    private static final String PRIMARY_POLICY = policy("primary", "");
    private static final String BACKUP_POLICY = policy("backup", "<allow-failback>true</allow-failback>");
    private static final String PRIMARY_LIVE = "AMQ221007";
    private static final String BACKUP_WAITING = "AMQ221109";

    private static Network network;
    private static ToxiproxyContainer toxiproxy;
    private static ToxiproxyClient proxies;
    private static int nextProxyPort = FIRST_PROXY_PORT;
    private static int generation;
    private static ChaosBroker single;
    private static String pairVolume;
    private static ChaosBroker primary;
    private static ChaosBroker backup;
    private static boolean pairDisturbed;

    @BeforeAll
    static void startProxy() {
        network = Network.newNetwork();
        int[] ports = new int[PROXY_PORTS + 1];
        ports[0] = 8474;
        for (int i = 0; i < PROXY_PORTS; i++) {
            ports[i + 1] = FIRST_PROXY_PORT + i;
        }
        toxiproxy = new ToxiproxyContainer(System.getProperty("toxiproxy.image", "ghcr.io/shopify/toxiproxy:2.12.0"))
                .withNetwork(network)
                .withExposedPorts(Arrays.stream(ports).boxed().toArray(Integer[]::new));
        toxiproxy.start();
        proxies = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        single = broker(SINGLE, "single", Optional.empty(), "", "Artemis Console available");
    }

    @AfterAll
    static void stop() {
        stopPair();
        if (single != null) {
            single.stop();
        }
        toxiproxy.stop();
        network.close();
    }

    @Override
    protected TckEnvironment environment() {
        return new TckEnvironment() {
            @Override
            public BrokerDriver driver() {
                return new ArtemisDriver();
            }

            @Override
            public ConnectRequest request() {
                return ArtemisChaosIT.request(List.of(single.spec(toxiproxy.getHost())));
            }

            @Override
            public ConnectRequest requestWithWrongPassword(String wrongPassword) {
                throw new UnsupportedOperationException("The chaos broker has no security");
            }

            @Override
            public KindId subscriptionKind() {
                return ArtemisKinds.QUEUE;
            }

            @Override
            public ResourceId subscriptionId(String name) {
                return new ResourceId(ArtemisKinds.QUEUE, ResourcePath.of(name));
            }

            @Override
            public Attributes subscriptionAttributes(String name) {
                return Attributes.builder()
                        .put(StandardAttributes.DURABLE, true)
                        .build();
            }

            @Override
            public MessageFilter nativeEquals(String name, String value) {
                return ArtemisFilters.selector(name + " = '" + value.replace("'", "''") + "'");
            }

            @Override
            public String nativeDialect() {
                return ArtemisFilters.DIALECT;
            }
        };
    }

    @Override
    protected TckChaos chaos() {
        single.heal();
        single.start();
        return new TckChaos() {
            @Override
            public ConnectRequest request() {
                return ArtemisChaosIT.request(List.of(single.spec(toxiproxy.getHost())));
            }

            @Override
            public ConnectRequest haRequest() {
                ensurePair();
                return ArtemisChaosIT.request(
                        List.of(primary.spec(toxiproxy.getHost()), backup.spec(toxiproxy.getHost())));
            }

            @Override
            public NodeId node() {
                return SINGLE;
            }

            @Override
            public NodeId primary() {
                return PRIMARY;
            }

            @Override
            public NodeId backup() {
                return BACKUP;
            }

            @Override
            public void cut(NodeId node) {
                member(node).ifPresent(ChaosBroker::cut);
                disturbed(node);
            }

            @Override
            public void heal(NodeId node) {
                member(node).ifPresent(ChaosBroker::heal);
            }

            @Override
            public void kill(NodeId node) {
                member(node).ifPresent(ChaosBroker::kill);
                disturbed(node);
            }

            @Override
            public void start(NodeId node) {
                member(node).ifPresent(ChaosBroker::start);
            }
        };
    }

    private static Optional<ChaosBroker> member(NodeId node) {
        if (node.equals(SINGLE)) {
            return Optional.of(single);
        }
        if (node.equals(PRIMARY)) {
            return Optional.ofNullable(primary);
        }
        return Optional.ofNullable(backup);
    }

    private static void disturbed(NodeId node) {
        if (!node.equals(SINGLE)) {
            pairDisturbed = true;
        }
    }

    /** A pair that is up, with the primary serving and the backup waiting; a disturbed one is replaced. */
    private static void ensurePair() {
        if (primary != null && !pairDisturbed) {
            return;
        }
        stopPair();
        generation++;
        pairVolume = "openbroker-chaos-" + UUID.randomUUID();
        ChaosBroker.prepareVolume(pairVolume);
        primary = broker(PRIMARY, "primary-" + generation, Optional.of(pairVolume), PRIMARY_POLICY, PRIMARY_LIVE);
        backup = broker(BACKUP, "backup-" + generation, Optional.of(pairVolume), BACKUP_POLICY, BACKUP_WAITING);
        pairDisturbed = false;
    }

    private static void stopPair() {
        if (backup != null) {
            backup.stop();
        }
        if (primary != null) {
            primary.stop();
        }
        if (pairVolume != null) {
            toxiproxy.getDockerClient().removeVolumeCmd(pairVolume).exec();
        }
        primary = null;
        backup = null;
        pairVolume = null;
    }

    private static ChaosBroker broker(NodeId id, String alias, Optional<String> volume, String haPolicy, String ready) {
        int listen = nextProxyPort++;
        if (listen >= FIRST_PROXY_PORT + PROXY_PORTS) {
            throw new IllegalStateException("The Toxiproxy container exposes no more ports");
        }
        try {
            Proxy proxy = proxies.createProxy(alias, "0.0.0.0:" + listen, alias + ":" + CORE);
            return ChaosBroker.start(
                    id,
                    alias,
                    network,
                    volume,
                    haPolicy,
                    ready,
                    proxy,
                    toxiproxy.getHost(),
                    toxiproxy.getMappedPort(listen));
        } catch (IOException e) {
            throw new IllegalStateException("Cannot proxy " + alias, e);
        }
    }

    private static ConnectRequest request(List<NodeSpec> nodes) {
        return new ConnectRequest(
                "artemis",
                ClusterSpec.ofNodes(nodes),
                CredentialSource.of(Credentials.usernamePassword("artemis", "artemis")),
                ConnectOptions.defaults().withTimeouts(Timeouts.fixed(Duration.ofSeconds(1), Duration.ofSeconds(2))));
    }

    private static String policy(String role, String extra) {
        return "<ha-policy><shared-store><" + role + "><failover-on-shutdown>true</failover-on-shutdown>" + extra + "</"
                + role + "></shared-store></ha-policy>";
    }
}
