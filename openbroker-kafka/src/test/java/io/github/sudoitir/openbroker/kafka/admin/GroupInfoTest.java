package io.github.sudoitir.openbroker.kafka.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.junit.jupiter.api.Test;

class GroupInfoTest {

    private static GroupInfo group(GroupType type, GroupState state, String assignor) {
        return new GroupInfo("g", type, state, OptionalInt.empty(), List.of(), OptionalInt.empty(), assignor, Set.of());
    }

    @Test
    void consumerClassicAndShareGroupsAreManageable() {
        assertThat(group(GroupType.CONSUMER, GroupState.STABLE, "").manageable())
                .isTrue();
        assertThat(group(GroupType.CLASSIC, GroupState.EMPTY, "").manageable()).isTrue();
        assertThat(group(GroupType.CLASSIC, GroupState.STABLE, "range").manageable())
                .isTrue();
        assertThat(group(GroupType.SHARE, GroupState.STABLE, "").manageable()).isTrue();
    }

    @Test
    void aStreamsGroupAndAStreamsAssignorGroupBelongToTheirApplication() {
        GroupInfo streams = group(GroupType.STREAMS, GroupState.STABLE, "");
        GroupInfo classicStreams = group(GroupType.CLASSIC, GroupState.STABLE, "stream");

        assertThat(streams.manageable()).isFalse();
        assertThat(streams.readOnlyReason()).contains("offsets belong to the application");
        assertThat(classicStreams.manageable()).isFalse();
        assertThat(classicStreams.readOnlyReason()).contains("Kafka Streams application");
        assertThat(group(GroupType.CLASSIC, GroupState.EMPTY, "").readOnlyReason())
                .isEmpty();
    }

    @Test
    void aGroupIsRebalancingInTheStatesOfAMembershipChange() {
        assertThat(group(GroupType.CLASSIC, GroupState.PREPARING_REBALANCE, "").rebalancing())
                .isTrue();
        assertThat(group(GroupType.CLASSIC, GroupState.COMPLETING_REBALANCE, "").rebalancing())
                .isTrue();
        assertThat(group(GroupType.CONSUMER, GroupState.ASSIGNING, "").rebalancing())
                .isTrue();
        assertThat(group(GroupType.CONSUMER, GroupState.RECONCILING, "").rebalancing())
                .isTrue();
        assertThat(group(GroupType.CLASSIC, GroupState.EMPTY, "").rebalancing()).isFalse();
        assertThat(group(GroupType.CLASSIC, GroupState.STABLE, "").rebalancing())
                .isFalse();
    }

    @Test
    void aListingWithoutATypeIsClassic() {
        assertThat(GroupInfo.typeOf(new GroupListing("g", Optional.empty(), "consumer", Optional.empty())))
                .isEqualTo(GroupType.CLASSIC);
        assertThat(GroupInfo.typeOf(new GroupListing("g", Optional.of(GroupType.SHARE), "", Optional.empty())))
                .isEqualTo(GroupType.SHARE);
    }
}
