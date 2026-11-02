package io.github.sudoitir.openbroker.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.github.sudoitir.openbroker.messaging.Count;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PreviewTest {

    private static Preview preview(EtagBasis basis) {
        return new Preview(
                List.of(),
                Set.of(),
                new Estimate(Count.exact(1), "{queue}"),
                "etag",
                basis,
                PreconditionEnforcement.CHECKED_BEFORE);
    }

    @Test
    void aPreviewStatesWhatItsEtagHolds() {
        assertThat(preview(EtagBasis.INCARNATION).basis()).isEqualTo(EtagBasis.INCARNATION);
        assertThat(preview(EtagBasis.CONFIGURATION).basis()).isEqualTo(EtagBasis.CONFIGURATION);
    }

    @Test
    void aPreviewWithoutABasisIsRefused() {
        assertThatNullPointerException().isThrownBy(() -> preview(null)).withMessageContaining("basis");
    }

    @Test
    void theBasisIsAClosedPairOfIncarnationAndConfiguration() {
        assertThat(EtagBasis.values()).containsExactly(EtagBasis.INCARNATION, EtagBasis.CONFIGURATION);
    }
}
