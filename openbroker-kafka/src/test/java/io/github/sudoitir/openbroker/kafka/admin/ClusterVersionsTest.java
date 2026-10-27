package io.github.sudoitir.openbroker.kafka.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.OptionalInt;
import org.junit.jupiter.api.Test;

class ClusterVersionsTest {

    @Test
    void kafka39StopsBelowTheFirstSupportedLevel() {
        assertThat(ClusterVersions.isSupported(OptionalInt.of(21))).isFalse();
    }

    @Test
    void kafka40SupportsItsFirstMetadataVersion() {
        assertThat(ClusterVersions.isSupported(OptionalInt.of(22))).isTrue();
        assertThat(ClusterVersions.isSupported(OptionalInt.of(25))).isTrue();
    }

    @Test
    void aClusterThatReportsNoMetadataVersionIsNotSupported() {
        assertThat(ClusterVersions.isSupported(OptionalInt.empty())).isFalse();
        assertThat(ClusterVersions.describe(OptionalInt.empty())).contains("metadata.version");
    }

    @Test
    void theRefusalNamesWhatWasFound() {
        assertThat(ClusterVersions.describe(OptionalInt.of(21)))
                .contains("metadata.version")
                .contains("21");
    }
}
