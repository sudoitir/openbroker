package io.github.sudoitir.openbroker.rabbitmq.admin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.ClusterSpec;
import io.github.sudoitir.openbroker.ConnectOptions;
import io.github.sudoitir.openbroker.CredentialSource;
import io.github.sudoitir.openbroker.Credentials;
import io.github.sudoitir.openbroker.Endpoint;
import io.github.sudoitir.openbroker.Plane;
import io.github.sudoitir.openbroker.access.AccessSnapshot;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.access.Assessment.Status;
import io.github.sudoitir.openbroker.access.Capability;
import io.github.sudoitir.openbroker.model.KindId;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RabbitMqAccessTest {

    private static final Map<String, String> JSON = Map.of("Content-Type", "application/json");
    private static final String PLUGINS_ALL = """
            [{"name":"rabbit@a","running":true,"enabled_plugins":["rabbitmq_management","rabbitmq_event_exchange",
            "rabbitmq_shovel","rabbitmq_shovel_management","rabbitmq_federation","rabbitmq_federation_management"]}]
            """;

    private FakeManagement server;

    @BeforeEach
    void start() {
        server = new FakeManagement();
    }

    @AfterEach
    void stop() {
        server.close();
    }

    private RabbitMqAccess access(boolean withData) {
        List<Endpoint> endpoints = withData
                ? List.of(server.endpoint(), Endpoint.of(Plane.DATA, "amqp", "localhost:5672"))
                : List.of(server.endpoint());
        return new RabbitMqAccess(new RabbitMqCluster(
                new ClusterSpec(List.of(), endpoints, Map.of()),
                CredentialSource.of(Credentials.usernamePassword("ops", "s3cret")),
                new FakeManagement.TestContext(ConnectOptions.defaults())));
    }

    private static ResourceId queue(KindId kind) {
        return ResourceId.of(kind.value(), "/", "orders");
    }

    private Assessment check(Capability capability) {
        return access(true).check(capability, Optional.empty());
    }

    private void whoami(String body) {
        server.respond("GET", "/api/whoami", 200, JSON, body);
    }

    @Test
    void browseIsOnStreamsAndTheAnswerComesFromTheKindWithNoRequest() {
        RabbitMqAccess access = access(true);
        assertThat(access.check(Capability.BROWSE, Optional.empty()).status()).isEqualTo(Status.AVAILABLE);
        Assessment classic = access.check(Capability.BROWSE, Optional.of(queue(RabbitMqKinds.CLASSIC_QUEUE)));
        Assessment quorum = access.check(Capability.BROWSE, Optional.of(queue(RabbitMqKinds.QUORUM_QUEUE)));
        assertThat(classic.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(classic.reason()).contains("Streams only");
        assertThat(classic.remedy()).isEmpty();
        assertThat(quorum.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(access.check(Capability.BROWSE, Optional.of(queue(RabbitMqKinds.STREAM)))
                        .status())
                .isEqualTo(Status.UNKNOWN);
        assertThat(server.received()).isEmpty();
    }

    @Test
    void withoutADataEndpointBrowsingSendingAndReceivingAreUnavailableWithARemedy() {
        RabbitMqAccess access = access(false);
        Assessment browse = access.check(Capability.BROWSE, Optional.of(queue(RabbitMqKinds.STREAM)));
        assertThat(browse.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(browse.remedy()).isPresent();
        assertThat(access.check(Capability.SEND, Optional.empty()).status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(access.check(Capability.RECEIVE, Optional.empty()).status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(server.received()).isEmpty();
    }

    @Test
    void sendAndReceiveAreSettledByTheFirstCallAndAStreamIsNotReceived() {
        RabbitMqAccess access = access(true);
        assertThat(access.check(Capability.SEND, Optional.empty()).status()).isEqualTo(Status.UNKNOWN);
        assertThat(access.check(Capability.RECEIVE, Optional.of(queue(RabbitMqKinds.QUORUM_QUEUE)))
                        .status())
                .isEqualTo(Status.UNKNOWN);
        Assessment stream = access.check(Capability.RECEIVE, Optional.of(queue(RabbitMqKinds.STREAM)));
        assertThat(stream.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(stream.reason()).contains("browse it");
        assertThat(server.received()).isEmpty();
    }

    @Test
    void messageActionsAreUnavailable() {
        RabbitMqAccess access = access(true);
        for (Capability capability :
                List.of(Capability.MOVE_MESSAGES, Capability.COPY_MESSAGES, Capability.DELETE_MESSAGES)) {
            Assessment assessment = access.check(capability, Optional.empty());
            assertThat(assessment.status()).isEqualTo(Status.UNAVAILABLE);
            assertThat(assessment.reason()).contains("cannot take one message");
        }
        assertThat(server.received()).isEmpty();
    }

    @Test
    void managementReadIsAvailableWhenWhoamiAnswers() {
        whoami("{\"name\":\"ops\",\"tags\":[\"administrator\"]}");
        assertThat(check(Capability.MANAGEMENT_READ).status()).isEqualTo(Status.AVAILABLE);
    }

    @Test
    void managementReadNamesRejectedCredentials() {
        server.respond("GET", "/api/whoami", 401, JSON, FakeManagement.fixture("error-login.json"));
        Assessment assessment = check(Capability.MANAGEMENT_READ);
        assertThat(assessment.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(assessment.remedy()).isEmpty();
    }

    @Test
    void managementReadOfAnAccountWithoutAManagementTagOffersTheTag() {
        server.respond("GET", "/api/whoami", 401, JSON, FakeManagement.fixture("error-not-admin.json"));
        Assessment assessment = check(Capability.MANAGEMENT_READ);
        assertThat(assessment.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(assessment.remedy().orElseThrow().snippet())
                .contains("set_user_tags")
                .contains("monitoring");
        assertThat(assessment.remedy().orElseThrow().mediaType()).isEqualTo("text/x-shellscript");
    }

    @Test
    void managementReadWithNothingListeningOffersTheManagementPlugin() {
        RabbitMqAccess access =
                new RabbitMqAccess(FakeManagement.cluster(Endpoint.of(Plane.MANAGEMENT, "http", "http://127.0.0.1:1")));
        Assessment assessment = access.check(Capability.MANAGEMENT_READ, Optional.empty());
        assertThat(assessment.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(assessment.remedy().orElseThrow().snippet()).contains("rabbitmq_management");
    }

    @Test
    void managementReadThatFailsOtherwiseIsUnknown() {
        server.respond("GET", "/api/whoami", 500, JSON, "{}");
        assertThat(check(Capability.MANAGEMENT_READ).status()).isEqualTo(Status.UNKNOWN);
    }

    @Test
    void managementWriteFollowsTheTags() {
        whoami("{\"name\":\"ops\",\"tags\":\"administrator,monitoring\"}");
        assertThat(check(Capability.MANAGEMENT_WRITE).status()).isEqualTo(Status.AVAILABLE);
    }

    @Test
    void managementWriteOfAPolicymakerDependsOnThePermission() {
        whoami("{\"name\":\"ops\",\"tags\":[\"policymaker\"]}");
        assertThat(check(Capability.MANAGEMENT_WRITE).status()).isEqualTo(Status.UNKNOWN);
    }

    @Test
    void managementWriteOfAMonitoringAccountIsUnavailableWithTheTagToGrant() {
        whoami("{\"name\":\"ops\",\"tags\":[\"monitoring\"]}");
        Assessment assessment = check(Capability.MANAGEMENT_WRITE);
        assertThat(assessment.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(assessment.remedy().orElseThrow().snippet()).isEqualTo("rabbitmqctl set_user_tags ops policymaker");
    }

    @Test
    void managementWriteWithoutTagsIsUnknown() {
        whoami("{\"name\":\"ops\",\"tags\":[]}");
        assertThat(check(Capability.MANAGEMENT_WRITE).status()).isEqualTo(Status.UNKNOWN);
    }

    @Test
    void writesAreNotAssessedWhenReadsAreUnavailable() {
        server.respond("GET", "/api/whoami", 401, JSON, FakeManagement.fixture("error-login.json"));
        assertThat(check(Capability.MANAGEMENT_WRITE).status()).isEqualTo(Status.UNKNOWN);
        assertThat(check(Capability.NOTIFICATIONS).status()).isEqualTo(Status.UNKNOWN);
    }

    @Test
    void notificationsNameTheEventExchangeWhenItIsUsable() {
        whoami("{\"name\":\"ops\",\"tags\":[\"administrator\"]}");
        server.respond("GET", "/api/nodes", 200, JSON, PLUGINS_ALL);
        Assessment assessment = check(Capability.NOTIFICATIONS);
        assertThat(assessment.status()).isEqualTo(Status.AVAILABLE);
        assertThat(assessment.reason()).isEqualTo("Events from amq.rabbitmq.event in vhost /");
    }

    @Test
    void notificationsPollWhenThePluginIsMissingOrThereIsNoDataEndpoint() {
        whoami("{\"name\":\"ops\",\"tags\":[\"administrator\"]}");
        server.respond(
                "GET",
                "/api/nodes",
                200,
                JSON,
                "[{\"name\":\"rabbit@a\",\"running\":true,\"enabled_plugins\":[\"rabbitmq_management\"]}]");
        Assessment noPlugin = check(Capability.NOTIFICATIONS);
        assertThat(noPlugin.status()).isEqualTo(Status.AVAILABLE);
        assertThat(noPlugin.reason()).startsWith("Polled every PT10S").contains("plugin is not enabled");
        Assessment noData = access(false).check(Capability.NOTIFICATIONS, Optional.empty());
        assertThat(noData.reason()).startsWith("Polled every PT10S").contains("no amqp endpoint");
    }

    @Test
    void notificationsPollWhenThePluginsCannotBeRead() {
        whoami("{\"name\":\"ops\",\"tags\":[\"administrator\"]}");
        server.respond("GET", "/api/nodes", 500, JSON, "{}");
        assertThat(check(Capability.NOTIFICATIONS).reason()).contains("could not be read");
    }

    @Test
    void theEventExchangeIsAvailableWhenItExistsAndOtherwiseOffersThePlugin() {
        whoami("{\"name\":\"ops\",\"tags\":[\"administrator\"]}");
        server.respond("GET", "/api/exchanges/%2F/amq.rabbitmq.event", 200, JSON, "{}");
        assertThat(check(RabbitMqAccess.EVENT_EXCHANGE).status()).isEqualTo(Status.AVAILABLE);
    }

    @Test
    void aMissingEventExchangeOffersThePlugin() {
        whoami("{\"name\":\"ops\",\"tags\":[\"administrator\"]}");
        Assessment assessment = check(RabbitMqAccess.EVENT_EXCHANGE);
        assertThat(assessment.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(assessment.remedy().orElseThrow().snippet())
                .isEqualTo("rabbitmq-plugins enable rabbitmq_event_exchange");
    }

    @Test
    void anEventExchangeThatCannotBeReadIsUnknown() {
        whoami("{\"name\":\"ops\",\"tags\":[\"administrator\"]}");
        server.respond("GET", "/api/exchanges/%2F/amq.rabbitmq.event", 403, JSON, "{}");
        assertThat(check(RabbitMqAccess.EVENT_EXCHANGE).status()).isEqualTo(Status.UNKNOWN);
    }

    @Test
    void shovelsAndFederationNeedTheirTwoPluginsOnEveryNode() {
        whoami("{\"name\":\"ops\",\"tags\":[\"administrator\"]}");
        server.json("GET", "/api/nodes", "nodes.json");
        Assessment shovels = check(RabbitMqAccess.SHOVELS);
        assertThat(shovels.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(shovels.remedy().orElseThrow().snippet())
                .isEqualTo("rabbitmq-plugins enable rabbitmq_shovel rabbitmq_shovel_management");
        assertThat(check(RabbitMqAccess.FEDERATION).status()).isEqualTo(Status.UNAVAILABLE);
    }

    @Test
    void shovelsAndFederationAreAvailableWithTheirPlugins() {
        whoami("{\"name\":\"ops\",\"tags\":[\"administrator\"]}");
        server.respond("GET", "/api/nodes", 200, JSON, PLUGINS_ALL);
        assertThat(check(RabbitMqAccess.SHOVELS).status()).isEqualTo(Status.AVAILABLE);
        assertThat(check(RabbitMqAccess.FEDERATION).status()).isEqualTo(Status.AVAILABLE);
    }

    @Test
    void pluginsThatCannotBeReadLeaveTheAnswerUnknown() {
        whoami("{\"name\":\"ops\",\"tags\":[\"administrator\"]}");
        server.respond("GET", "/api/nodes", 403, JSON, "{}");
        assertThat(check(RabbitMqAccess.SHOVELS).status()).isEqualTo(Status.UNKNOWN);
        server.respond("GET", "/api/nodes", 500, JSON, "{}");
    }

    @Test
    void streamsAreAlwaysAvailableOnFourPointX() {
        whoami("{\"name\":\"ops\",\"tags\":[\"administrator\"]}");
        server.respond("GET", "/api/overview", 200, JSON, "{\"rabbitmq_version\":\"4.0.5\"}");
        assertThat(check(RabbitMqAccess.STREAMS).status()).isEqualTo(Status.AVAILABLE);
    }

    @Test
    void streamsOnThreePointThirteenFollowTheFeatureFlag() {
        whoami("{\"name\":\"ops\",\"tags\":[\"administrator\"]}");
        server.json("GET", "/api/overview", "overview.json");
        server.respond("GET", "/api/feature-flags", 200, JSON, "[{\"name\":\"stream_queue\",\"state\":\"enabled\"}]");
        assertThat(check(RabbitMqAccess.STREAMS).status()).isEqualTo(Status.AVAILABLE);
    }

    @Test
    void streamsWithTheFlagOffOfferToEnableIt() {
        whoami("{\"name\":\"ops\",\"tags\":[\"administrator\"]}");
        server.json("GET", "/api/overview", "overview.json");
        server.respond("GET", "/api/feature-flags", 200, JSON, "[{\"name\":\"stream_queue\",\"state\":\"disabled\"}]");
        Assessment assessment = check(RabbitMqAccess.STREAMS);
        assertThat(assessment.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(assessment.remedy().orElseThrow().snippet()).contains("enable_feature_flag stream_queue");
    }

    @Test
    void streamsAreUnknownWhenTheFlagsCannotBeReadOrAreNotListed() {
        whoami("{\"name\":\"ops\",\"tags\":[\"administrator\"]}");
        server.json("GET", "/api/overview", "overview.json");
        server.respond("GET", "/api/feature-flags", 200, JSON, "[]");
        assertThat(check(RabbitMqAccess.STREAMS).status()).isEqualTo(Status.UNKNOWN);
        server.respond("GET", "/api/feature-flags", 403, JSON, "{}");
        assertThat(check(RabbitMqAccess.STREAMS).status()).isEqualTo(Status.UNKNOWN);
    }

    @Test
    void aCapabilityTheDriverDoesNotKnowIsUnknown() {
        whoami("{\"name\":\"ops\",\"tags\":[\"administrator\"]}");
        assertThat(check(new Capability("other.thing")).status()).isEqualTo(Status.UNKNOWN);
    }

    @Test
    void probeAnswersEveryCapabilityWithOneWhoami() {
        whoami("{\"name\":\"ops\",\"tags\":[\"administrator\"]}");
        server.respond("GET", "/api/nodes", 200, JSON, PLUGINS_ALL);
        server.respond("GET", "/api/exchanges/%2F/amq.rabbitmq.event", 200, JSON, "{}");
        server.respond("GET", "/api/overview", 200, JSON, "{\"rabbitmq_version\":\"4.0.5\"}");
        AccessSnapshot snapshot = access(true).probe();
        assertThat(snapshot.assessments())
                .containsKeys(
                        Capability.MANAGEMENT_READ,
                        Capability.MANAGEMENT_WRITE,
                        Capability.NOTIFICATIONS,
                        Capability.BROWSE,
                        Capability.SEND,
                        Capability.RECEIVE,
                        Capability.MOVE_MESSAGES,
                        Capability.COPY_MESSAGES,
                        Capability.DELETE_MESSAGES,
                        RabbitMqAccess.EVENT_EXCHANGE,
                        RabbitMqAccess.SHOVELS,
                        RabbitMqAccess.FEDERATION,
                        RabbitMqAccess.STREAMS);
        assertThat(server.received().stream().filter(r -> r.path().equals("/api/whoami")))
                .hasSize(1);
        assertThat(snapshot.of(Capability.MANAGEMENT_WRITE).status()).isEqualTo(Status.AVAILABLE);
    }

    @Test
    void probeOfAnUnreadableClusterAssessesNothingBeyondTheKinds() {
        server.respond("GET", "/api/whoami", 401, JSON, FakeManagement.fixture("error-login.json"));
        AccessSnapshot snapshot = access(true).probe();
        assertThat(snapshot.of(Capability.MANAGEMENT_READ).status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(snapshot.of(Capability.MANAGEMENT_WRITE).status()).isEqualTo(Status.UNKNOWN);
        assertThat(snapshot.of(Capability.NOTIFICATIONS).status()).isEqualTo(Status.UNKNOWN);
        assertThat(snapshot.assessments()).doesNotContainKey(RabbitMqAccess.SHOVELS);
    }
}
