package io.github.sudoitir.openbroker.rabbitmq.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.model.Condition;
import io.github.sudoitir.openbroker.model.ConditionStatus;
import io.github.sudoitir.openbroker.model.ConditionTypes;
import io.github.sudoitir.openbroker.model.PartitionId;
import io.github.sudoitir.openbroker.model.Placement;
import io.github.sudoitir.openbroker.model.PlacementRole;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class PlacementsTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final JsonNode QUEUES = JSON.readTree(FakeManagement.fixture("queues-detail.json"));

    private static JsonNode queue(String name) {
        for (JsonNode item : QUEUES.path("items")) {
            if (name.equals(item.path("name").asString())) {
                return item;
            }
        }
        throw new IllegalArgumentException(name);
    }

    @Test
    void aQuorumQueueHasALeaderAndFollowersMarkedInSyncByTheOnlineMembers() {
        List<Placement> placements = Placements.of(RabbitMqKinds.QUORUM_QUEUE, queue("payments"));

        assertThat(placements)
                .extracting(
                        p -> p.node().value(), Placement::role, p -> p.inSync().orElseThrow())
                .containsExactly(
                        tuple("rabbit@alpha", PlacementRole.LEADER, true),
                        tuple("rabbit@beta", PlacementRole.FOLLOWER, true),
                        tuple("rabbit@gamma", PlacementRole.FOLLOWER, false));
        assertThat(placements).allSatisfy(p -> assertThat(p.partition()).isEmpty());
    }

    @Test
    void anOfflineMemberMakesTheQueueUnderReplicated() {
        Condition condition = Placements.underReplicated(RabbitMqKinds.QUORUM_QUEUE, queue("payments"))
                .orElseThrow();

        assertThat(condition.type()).isEqualTo(ConditionTypes.UNDER_REPLICATED);
        assertThat(condition.status()).isEqualTo(ConditionStatus.TRUE);
        assertThat(condition.message()).contains("2 of 3").contains("rabbit@gamma");
    }

    @Test
    void aStreamIsOneLogWithItsLeaderFirstAndAllMembersInSync() {
        List<Placement> placements = Placements.of(RabbitMqKinds.STREAM, queue("audit"));

        assertThat(placements.getFirst().node()).isEqualTo(NodeId.of("rabbit@beta"));
        assertThat(placements.getFirst().role()).isEqualTo(PlacementRole.LEADER);
        assertThat(placements).hasSize(3).allSatisfy(p -> {
            assertThat(p.inSync()).contains(true);
            assertThat(p.partition()).contains(PartitionId.of("0"));
        });
        assertThat(Placements.underReplicated(RabbitMqKinds.STREAM, queue("audit"))
                        .orElseThrow()
                        .status())
                .isEqualTo(ConditionStatus.FALSE);
    }

    @Test
    void aClassicQueueIsOneInstance() {
        List<Placement> placements = Placements.of(RabbitMqKinds.CLASSIC_QUEUE, queue("orders"));

        assertThat(placements).hasSize(1);
        assertThat(placements.getFirst().role()).isEqualTo(PlacementRole.INSTANCE);
        assertThat(placements.getFirst().node()).isEqualTo(NodeId.of("rabbit@alpha"));
        assertThat(Placements.underReplicated(RabbitMqKinds.CLASSIC_QUEUE, queue("orders")))
                .isEmpty();
    }

    @Test
    void aMirroredClassicQueueHasItsMirrorsAsFollowersInSyncWhenSynchronised() {
        List<Placement> placements = Placements.of(RabbitMqKinds.CLASSIC_QUEUE, queue("legacy"));

        assertThat(placements)
                .extracting(
                        p -> p.node().value(), Placement::role, p -> p.inSync().orElseThrow())
                .containsExactly(
                        tuple("rabbit@alpha", PlacementRole.LEADER, true),
                        tuple("rabbit@beta", PlacementRole.FOLLOWER, true),
                        tuple("rabbit@gamma", PlacementRole.FOLLOWER, false));
        assertThat(Placements.underReplicated(RabbitMqKinds.CLASSIC_QUEUE, queue("legacy"))
                        .orElseThrow()
                        .status())
                .isEqualTo(ConditionStatus.TRUE);
    }

    @Test
    void aReducedListingWithoutOnlineLeavesSyncUnknown() {
        JsonNode reduced = JSON.readTree(
                "{\"node\":\"rabbit@alpha\",\"leader\":\"rabbit@alpha\",\"members\":[\"rabbit@alpha\",\"rabbit@beta\"]}");

        assertThat(Placements.of(RabbitMqKinds.QUORUM_QUEUE, reduced))
                .allSatisfy(p -> assertThat(p.inSync()).isEmpty());
        assertThat(Placements.underReplicated(RabbitMqKinds.QUORUM_QUEUE, reduced))
                .isEqualTo(Optional.empty());
    }

    @Test
    void aQuorumQueueWithoutMembersFallsBackToItsNodeAndOneWithNoNodeHasNone() {
        JsonNode bare = JSON.readTree("{\"node\":\"rabbit@alpha\"}");

        assertThat(Placements.of(RabbitMqKinds.QUORUM_QUEUE, bare))
                .extracting(Placement::role)
                .containsExactly(PlacementRole.INSTANCE);
        assertThat(Placements.of(RabbitMqKinds.QUORUM_QUEUE, JSON.readTree("{}")))
                .isEmpty();
    }
}
