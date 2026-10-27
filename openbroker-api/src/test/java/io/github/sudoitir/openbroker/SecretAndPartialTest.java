package io.github.sudoitir.openbroker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SecretAndPartialTest {

    @Test
    void aCompletePartialHasEveryNodeApplied() {
        Partial<String> p = Partial.complete("x", List.of(NodeId.of("a"), NodeId.of("b")));
        assertThat(p.isComplete()).isTrue();
        assertThat(p.orThrow()).isEqualTo("x");
    }

    @Test
    void aPartialWithNoAnsweringNodeThrowsTheFailure() {
        BrokerException down = new BrokerException(
                Status.UNAVAILABLE, ErrorInfo.of(ErrorReasons.UNREACHABLE), Dispatch.NOT_SENT, NodeId.of("a"), "down");
        Partial<List<String>> p =
                new Partial<>(List.of(), Map.of(NodeId.of("a"), NodeOutcome.FAILED), Map.of(NodeId.of("a"), down));
        assertThat(p.isComplete()).isFalse();
        assertThatThrownBy(p::orThrow).isSameAs(down);
    }

    @Test
    void anInDoubtFailureIsNotSafeToRetry() {
        BrokerException e = new BrokerException(
                Status.UNAVAILABLE, ErrorInfo.of(ErrorReasons.UNREACHABLE), Dispatch.IN_DOUBT, null, "no answer");
        assertThat(e.safeToRetry()).isFalse();
        assertThat(e.at(NodeId.of("n1")).node()).contains(NodeId.of("n1"));
    }
}
