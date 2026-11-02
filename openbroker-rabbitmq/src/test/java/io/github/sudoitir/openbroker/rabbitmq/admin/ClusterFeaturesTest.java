package io.github.sudoitir.openbroker.rabbitmq.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ClusterFeaturesTest {

    private FakeManagement server;

    @BeforeEach
    void start() {
        server = new FakeManagement();
    }

    @AfterEach
    void stop() {
        server.close();
    }

    private ClusterFeatures features() {
        return FakeManagement.cluster(server.endpoint()).features();
    }

    private void healthy() {
        server.json("GET", "/api/overview", "overview.json")
                .json("GET", "/api/nodes", "nodes.json")
                .json("GET", "/api/whoami", "whoami.json");
    }

    @Test
    void readsTheVersionAndTheProduct() {
        healthy();
        ClusterFeatures f = features();
        assertThat(f.version().text()).isEqualTo("3.13.7");
        assertThat(f.version().major()).isEqualTo(3);
        assertThat(f.version().minor()).isEqualTo(13);
        assertThat(f.product()).isEqualTo("RabbitMQ");
        assertThat(f.productVersion()).isEqualTo("3.13.7");
        assertThat(f.clusterName()).isEqualTo("rabbit@alpha");
        assertThat(f.atLeast(3, 13)).isTrue();
        assertThat(f.atLeast(4, 0)).isFalse();
    }

    @Test
    void acceptsTheSupportedRange() {
        healthy();
        ClusterFeatures f = features();
        assertThat(f.require("[3.13.0,)")).isSameAs(f);
        assertThatCode(() -> f.require("[3.13.0,4.0.0)")).doesNotThrowAnyException();
    }

    @Test
    void refusesAnOlderVersionNamingWhatItFound() {
        server.json("GET", "/api/overview", "overview-3.12.json");
        ClusterFeatures subject = features();
        assertThatThrownBy(() -> subject.require("[3.13.0,)")).isInstanceOfSatisfying(BrokerException.class, e -> {
            assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
            assertThat(e.reason()).isEqualTo(ErrorReasons.UNSUPPORTED_VERSION);
            assertThat(e.getMessage()).contains("3.12.14").contains("[3.13.0,)");
        });
    }

    @Test
    void refusesAVersionAboveTheRangeAndOnTheExclusiveBound() {
        healthy();
        ClusterFeatures subject = features();
        assertThatThrownBy(() -> subject.require("[3.0.0,3.13.7)")).isInstanceOf(BrokerException.class);
        assertThatThrownBy(() -> subject.require("(3.13.7,)")).isInstanceOf(BrokerException.class);
        assertThatCode(() -> subject.require("[3.13.7,3.13.7]")).doesNotThrowAnyException();
        assertThatCode(() -> subject.require("3.13.0")).doesNotThrowAnyException();
    }

    @Test
    void readsAnIntervalWithBlanksAndSurvivesALongMalformedOne() {
        healthy();
        ClusterFeatures features = features();
        assertThatCode(() -> features.require("[ 3.13.0 , )")).doesNotThrowAnyException();
        assertThatCode(() -> features.require("[3.13.0,,4.0)")).doesNotThrowAnyException();
        String adversarial = "[" + " ".repeat(200_000) + "1 2" + " ".repeat(200_000) + ",";
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> features.require(adversarial));
    }

    @Test
    void aReleaseCandidateCountsByItsNumbers() {
        server.respond("GET", "/api/overview", 200, Map.of(), "{\"rabbitmq_version\":\"4.0.0-rc.2\"}");
        ClusterFeatures subject = features();
        assertThatCode(() -> subject.require("[3.13.0,)")).doesNotThrowAnyException();
    }

    @Test
    void anAnswerWithoutAVersionIsNotRabbitMq() {
        server.json("GET", "/api/overview", "overview-no-version.json");
        ClusterFeatures subject = features();
        assertThatThrownBy(() -> subject.require("[3.13.0,)"))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.reason()).isEqualTo(ErrorReasons.UNEXPECTED_SYSTEM));
    }

    @Test
    void anAnswerThatIsNotJsonOrNotAnObjectIsNotRabbitMq() {
        server.respond("GET", "/api/overview", 200, Map.of(), "<html></html>");
        assertThatThrownBy(features()::version)
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.reason()).isEqualTo(ErrorReasons.UNEXPECTED_SYSTEM));
        server.close();
        server = new FakeManagement();
        server.respond("GET", "/api/overview", 200, Map.of(), "[1]");
        assertThatThrownBy(features()::version)
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.reason()).isEqualTo(ErrorReasons.UNEXPECTED_SYSTEM));
    }

    @Test
    void aVersionItCannotReadIsNotRabbitMq() {
        server.respond("GET", "/api/overview", 200, Map.of(), "{\"rabbitmq_version\":\"soon\"}");
        assertThatThrownBy(features()::version).isInstanceOf(BrokerException.class);
    }

    @Test
    void aFailedOverviewKeepsItsOwnReason() {
        server.respond("GET", "/api/overview", 500, Map.of(), "");
        assertThatThrownBy(features()::version)
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.reason()).isEqualTo(ErrorReasons.BAD_RESPONSE));
    }

    @Test
    void aPluginCountsOnlyWhenEveryNodeHasIt() {
        healthy();
        ClusterFeatures f = features();
        assertThat(f.hasPlugin(ClusterFeatures.PLUGIN_MANAGEMENT)).isTrue();
        assertThat(f.eventExchange()).isTrue();
        assertThat(f.hasPlugin(ClusterFeatures.PLUGIN_SHOVEL))
                .as("a node that is down is not counted")
                .isTrue();
        assertThat(f.shovels())
                .as("the shovel management plugin is on one node only")
                .isFalse();
        assertThat(f.federation()).isFalse();
        assertThat(f.streamPlugin()).as("stream is on one node only").isFalse();
    }

    @Test
    void shovelsAndFederationNeedBothPlugins() {
        server.json("GET", "/api/overview", "overview.json")
                .respond(
                        "GET",
                        "/api/nodes",
                        200,
                        Map.of(),
                        "[{\"name\":\"n\",\"enabled_plugins\":[\"rabbitmq_shovel\",\"rabbitmq_shovel_management\","
                                + "\"rabbitmq_federation\",\"rabbitmq_federation_management\",\"rabbitmq_stream\"]}]")
                .json("GET", "/api/whoami", "whoami.json");
        ClusterFeatures f = features();
        assertThat(f.shovels()).isTrue();
        assertThat(f.federation()).isTrue();
        assertThat(f.streamPlugin()).isTrue();
    }

    @Test
    void readsTheTagsAsAListOrAsText() {
        healthy();
        assertThat(features().tags()).containsExactlyInAnyOrder("administrator", "monitoring");
        server.close();
        server = new FakeManagement();
        server.json("GET", "/api/overview", "overview.json")
                .json("GET", "/api/nodes", "nodes.json")
                .json("GET", "/api/whoami", "whoami-string.json");
        assertThat(features().tags()).containsExactlyInAnyOrder("management", "policymaker");
    }

    @Test
    void anAccountThatMayNotReadNodesOrItselfLeavesThemUnknown() {
        server.json("GET", "/api/overview", "overview.json")
                .respond("GET", "/api/nodes", 403, Map.of(), "{}")
                .respond("GET", "/api/whoami", 401, Map.of(), "{\"reason\":\"Not management user\"}");
        ClusterFeatures f = features();
        assertThat(f.version().text()).isEqualTo("3.13.7");
        assertThat(f.plugins()).isEmpty();
        assertThat(f.tags()).isEmpty();
    }

    @Test
    void otherFailuresOfTheSecondaryReadsPropagate() {
        server.json("GET", "/api/overview", "overview.json").respond("GET", "/api/nodes", 503, Map.of(), "");
        assertThatThrownBy(features()::plugins).isInstanceOf(BrokerException.class);
        server.close();
        server = new FakeManagement();
        server.json("GET", "/api/overview", "overview.json")
                .json("GET", "/api/nodes", "nodes.json")
                .respond("GET", "/api/whoami", 503, Map.of(), "");
        ClusterFeatures features = features();
        assertThatThrownBy(features::tags).isInstanceOf(BrokerException.class);
    }

    @Test
    void readsOnceUntilReset() {
        healthy();
        ClusterFeatures f = features();
        f.version();
        f.version();
        f.tags();
        f.tags();
        f.plugins();
        f.plugins();
        assertThat(server.received()).hasSize(3);
        f.reset();
        f.version();
        assertThat(server.received()).hasSize(4);
    }
}
