package io.github.sudoitir.openbroker.artemis.internal;

import static org.assertj.core.api.Assertions.assertThat;

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
import io.github.sudoitir.openbroker.model.ResourceClass;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.Scope;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

/**
 * The Jolokia requests a facet call produces, byte for byte. The expected bodies are requests recorded
 * from a management tool's integration tests against a real broker, before that tool moved onto this
 * driver: a driver release that changes what it asks of a broker fails here rather than in production.
 */
class WireShapesTest {

    private static final String SEARCH = "{\"mbean\":\"org.apache.activemq.artemis:broker=*\",\"type\":\"search\"}";

    private static final String FIRST_QUEUE_PAGE =
            "{\"arguments\":[\"{\\\"field\\\":\\\"\\\",\\\"operation\\\":\\\"\\\","
                    + "\\\"value\\\":\\\"\\\"}\",1,200],\"mbean\":\"org.apache.activemq.artemis:broker=\\\"primary\\\"\","
                    + "\"operation\":\"listQueues(java.lang.String,int,int)\",\"type\":\"exec\"}";

    private static final String HA_READ = "{\"attribute\":[\"Active\",\"Started\",\"Backup\",\"ReplicaSync\","
            + "\"NodeID\",\"Clustered\",\"Version\"],\"mbean\":\"org.apache.activemq.artemis:broker=\\\"primary\\\"\","
            + "\"type\":\"read\"}";

    /** The recording kept no key order, so the bodies are compared as JSON. */
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final FixtureServer server = new FixtureServer();
    private final BrokerDrivers drivers = BrokerDrivers.load();

    @AfterEach
    void stop() {
        drivers.close();
        server.close();
    }

    @Test
    void aPageOfSubscriptionsFindsTheBrokerOnceThenReadsOnePageOfQueues() {
        server.json("search-broker.json").json("list-queues.json");

        try (ClusterConnection cluster = connect()) {
            cluster.resources()
                    .read(ResourceQuery.of(ResourceClass.SUBSCRIPTION).limit(200))
                    .orThrow();
        }

        assertThat(server.received())
                .extracting(r -> JSON.readTree(r.body()))
                .containsExactly(JSON.readTree(SEARCH), JSON.readTree(FIRST_QUEUE_PAGE));
    }

    @Test
    void readingANodesHighAvailabilityStateIsOneReadOfSevenAttributes() {
        server.json("search-broker.json").json("ha-read-primary.json");

        try (ClusterConnection cluster = connect()) {
            cluster.nodes().describe(Scope.whole()).orThrow();
        }

        assertThat(server.received())
                .extracting(r -> JSON.readTree(r.body()))
                .containsExactly(JSON.readTree(SEARCH), JSON.readTree(HA_READ));
    }

    private ClusterConnection connect() {
        return drivers.connect(new ConnectRequest(
                "artemis",
                ClusterSpec.ofNodes(List.of(new NodeSpec(
                        NodeId.of("n1"),
                        List.of(Endpoint.of(
                                Plane.MANAGEMENT, "jolokia", server.endpoint().toString()))))),
                CredentialSource.of(Credentials.of("artemis", "artemis")),
                ConnectOptions.defaults()));
    }
}
