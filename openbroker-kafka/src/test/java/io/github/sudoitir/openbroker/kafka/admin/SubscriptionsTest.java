package io.github.sudoitir.openbroker.kafka.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.junit.jupiter.api.Test;

class SubscriptionsTest {

    private static final ResourceId ID = ResourceId.of("kafka.group-topic", "g", "t");

    private static GroupInfo group(GroupState state, List<GroupInfo.Member> members) {
        return new GroupInfo(
                "g", GroupType.CLASSIC, state, OptionalInt.empty(), members, OptionalInt.empty(), "", Set.of());
    }

    @Test
    void anEmptySettledGroupMayBeMoved() {
        assertThatCode(() -> Subscriptions.requireIdle(ID, group(GroupState.EMPTY, List.of()), "move"))
                .doesNotThrowAnyException();
    }

    @Test
    void aGroupWithMembersIsRefusedNamingThem() {
        GroupInfo busy = group(GroupState.STABLE, List.of(new GroupInfo.Member("m-1", "app", "10.0.0.1", Set.of())));

        assertThatThrownBy(() -> Subscriptions.requireIdle(ID, busy, "move the offsets of"))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                    assertThat(e.reason()).isEqualTo(ErrorReasons.NOT_EMPTY);
                    assertThat(e.getMessage()).contains("has active members").contains("app@10.0.0.1");
                });
    }

    @Test
    void aGroupThatIsRebalancingIsRefusedEvenWithoutMembers() {
        assertThatThrownBy(
                        () -> Subscriptions.requireIdle(ID, group(GroupState.PREPARING_REBALANCE, List.of()), "move"))
                .isInstanceOfSatisfying(BrokerException.class, e -> {
                    assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
                    assertThat(e.reason()).isEqualTo(ErrorReasons.NOT_EMPTY);
                    assertThat(e.getMessage()).contains("is rebalancing").contains("Try again once it settles");
                });
    }
}
