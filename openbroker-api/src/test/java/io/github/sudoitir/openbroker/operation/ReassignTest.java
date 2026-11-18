package io.github.sudoitir.openbroker.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.PartitionId;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ReassignTest {

    @Test
    void aMapAloneUsesEmptyOptionsAndIsCopied() {
        Map<PartitionId, List<NodeId>> assignments = Map.of(PartitionId.of("0"), List.of(NodeId.of("1")));

        Operation.Reassign reassign = new Operation.Reassign(assignments);

        assertThat(reassign.assignments()).isEqualTo(assignments);
        assertThat(reassign.options()).isEqualTo(Attributes.empty());
        assertThat(reassign.options()).isSameAs(Attributes.empty());
    }

    @Test
    void optionsAreRequired() {
        assertThatNullPointerException()
                .isThrownBy(() -> new Operation.Reassign(Map.of(), null))
                .withMessageContaining("options");
    }

    @Test
    void theCanonicalConstructorKeepsTheOptions() {
        Attributes options = Attributes.builder()
                .putRaw("replication.throttle.bytes_per_second", 1024L)
                .build();

        Operation.Reassign reassign = new Operation.Reassign(Map.of(), options);

        assertThat(reassign.options()).isEqualTo(options);
    }
}
