package io.github.sudoitir.openbroker.support.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.DeduplicationScope;
import io.github.sudoitir.openbroker.messaging.Guarantee;
import io.github.sudoitir.openbroker.messaging.OrderingScope;
import io.github.sudoitir.openbroker.messaging.PrefetchBound;
import io.github.sudoitir.openbroker.messaging.TransportCapabilities;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class DeclaredTest {

    private static final TransportCapabilities SESSION_ONLY = new TransportCapabilities(
            Set.of(Guarantee.AT_LEAST_ONCE),
            DeduplicationScope.SESSION,
            Set.of(OrderingScope.NONE),
            Set.of(PrefetchBound.MESSAGES),
            Map.of(),
            false);

    private static void refused(Runnable ask, String what) {
        assertThatThrownBy(ask::run).isInstanceOfSatisfying(BrokerException.class, e -> {
            assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED);
            assertThat(e.dispatch()).isEqualTo(Dispatch.NOT_SENT);
            assertThat(e.getMessage()).contains("X cannot").contains(what);
        });
    }

    @Test
    void anAskWithinTheDeclarationPasses() {
        assertThatCode(() -> Declared.require(
                        "X",
                        SESSION_ONLY,
                        Guarantee.AT_LEAST_ONCE,
                        DeduplicationScope.SESSION,
                        OrderingScope.NONE,
                        Set.of(OrderingScope.NONE)))
                .doesNotThrowAnyException();
    }

    @Test
    void aDeduplicationAskAboveTheDeclaredScopeIsRefused() {
        refused(
                () -> Declared.require(
                        "X",
                        SESSION_ONLY,
                        Guarantee.AT_LEAST_ONCE,
                        DeduplicationScope.WINDOW,
                        OrderingScope.NONE,
                        Set.of(OrderingScope.NONE)),
                "WINDOW");
    }

    @Test
    void anUndeclaredGuaranteeIsRefused() {
        refused(
                () -> Declared.require(
                        "X",
                        SESSION_ONLY,
                        Guarantee.TRANSACTED,
                        DeduplicationScope.NONE,
                        OrderingScope.NONE,
                        Set.of(OrderingScope.NONE)),
                "TRANSACTED");
    }

    @Test
    void anOrderingTheKindCannotKeepIsRefused() {
        refused(
                () -> Declared.require(
                        "X",
                        SESSION_ONLY,
                        Guarantee.AT_LEAST_ONCE,
                        DeduplicationScope.NONE,
                        OrderingScope.GROUP,
                        Set.of(OrderingScope.NONE)),
                "GROUP");
    }
}
