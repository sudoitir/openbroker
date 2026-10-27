package io.github.sudoitir.openbroker.kafka;

import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.ConnectRequest;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.Timeouts;
import io.github.sudoitir.openbroker.kafka.admin.KafkaCluster;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.tck.ChaosTck;
import io.github.sudoitir.openbroker.tck.TckChaos;
import io.github.sudoitir.openbroker.tck.TckEnvironment;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * The chaos TCK against a three-broker cluster of the image the build names ({@code kafka.image}),
 * every broker behind Toxiproxy. The cluster as a whole is the TCK's node: cutting it holds the
 * traffic to every broker, and killing it stops them all. The serving node of a subscription is its
 * partition's leader and the successor an in-sync follower; the chaos topics have one partition.
 */
class KafkaChaosIT extends ChaosTck {

    private static KafkaChaosCluster cluster;

    @BeforeAll
    static void startCluster() {
        cluster = new KafkaChaosCluster(
                KafkaTestCluster.image(), System.getProperty("toxiproxy.image", "ghcr.io/shopify/toxiproxy:2.12.0"));
    }

    @AfterAll
    static void stopCluster() {
        cluster.close();
    }

    @Override
    protected TckEnvironment environment() {
        return new KafkaTckEnvironment(KafkaChaosIT::request);
    }

    @Override
    protected TckChaos chaos() {
        cluster.heal(KafkaChaosCluster.all());
        cluster.start(KafkaChaosCluster.all());
        return new TckChaos() {
            @Override
            public ConnectRequest request() {
                return KafkaChaosIT.request();
            }

            @Override
            public ConnectRequest haRequest() {
                return request();
            }

            @Override
            public NodeId node() {
                return KafkaCluster.CLUSTER;
            }

            @Override
            public NodeId serving(ResourceId subscription) {
                return cluster.leader(topic(subscription));
            }

            @Override
            public NodeId successor(ResourceId subscription) {
                return cluster.follower(topic(subscription));
            }

            @Override
            public void cut(NodeId node) {
                cluster.cut(members(node));
            }

            @Override
            public void heal(NodeId node) {
                cluster.heal(members(node));
            }

            @Override
            public void kill(NodeId node) {
                cluster.kill(members(node));
            }

            @Override
            public void start(NodeId node) {
                cluster.start(members(node));
            }
        };
    }

    /** The brokers a node stands for: the whole cluster, or one broker. */
    private static List<NodeId> members(NodeId node) {
        return node.equals(KafkaCluster.CLUSTER) ? KafkaChaosCluster.all() : List.of(node);
    }

    private static String topic(ResourceId subscription) {
        return subscription.path().segments().get(1);
    }

    private static ConnectRequest request() {
        return new ConnectRequest(
                KafkaDriver.SYSTEM,
                cluster.spec(),
                CredentialSource.of(Credentials.usernamePassword(KafkaChaosCluster.USER, KafkaChaosCluster.PASSWORD)),
                ConnectOptions.defaults().withTimeouts(Timeouts.fixed(Duration.ofSeconds(1), Duration.ofSeconds(2))));
    }
}
