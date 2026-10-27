package io.github.sudoitir.openbroker.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class DeduplicationScopeTest {

    @Test
    void aStrongerScopeSatisfiesAWeakerAsk() {
        assertThat(DeduplicationScope.WINDOW.satisfies(DeduplicationScope.SESSION))
                .isTrue();
        assertThat(DeduplicationScope.SESSION.satisfies(DeduplicationScope.SESSION))
                .isTrue();
        assertThat(DeduplicationScope.SESSION.satisfies(DeduplicationScope.NONE))
                .isTrue();
    }

    @Test
    void aWeakerScopeDoesNotSatisfyAStrongerAsk() {
        assertThat(DeduplicationScope.NONE.satisfies(DeduplicationScope.SESSION))
                .isFalse();
        assertThat(DeduplicationScope.SESSION.satisfies(DeduplicationScope.WINDOW))
                .isFalse();
    }
}
