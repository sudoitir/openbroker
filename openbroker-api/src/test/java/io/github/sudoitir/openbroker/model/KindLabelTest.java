package io.github.sudoitir.openbroker.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class KindLabelTest {

    @Test
    void keepsItsSingularAndPlural() {
        KindLabel label = KindLabel.of("Group on topic", "Groups on topics");
        assertThat(label.singular()).isEqualTo("Group on topic");
        assertThat(label.plural()).isEqualTo("Groups on topics");
    }

    @Test
    void refusesABlankSingularOrPlural() {
        assertThatThrownBy(() -> KindLabel.of(" ", "Topics")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KindLabel.of("Topic", "")).isInstanceOf(IllegalArgumentException.class);
    }
}
