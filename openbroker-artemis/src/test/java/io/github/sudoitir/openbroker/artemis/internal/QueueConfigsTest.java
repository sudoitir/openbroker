package io.github.sudoitir.openbroker.artemis.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.artemis.ArtemisKinds;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import java.util.Map;
import org.junit.jupiter.api.Test;

class QueueConfigsTest {

    @Test
    void aDeadLetterAddressIsRefusedRatherThanIgnored() {
        Attributes asked =
                Attributes.builder().put(StandardAttributes.DEAD_LETTER, "DLQ").build();

        assertThatThrownBy(() -> QueueConfigs.toConfig("q", asked, true))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT));
    }

    @Test
    void creatingTheAddressIsDecidedOnCreateOnly() {
        Attributes asked = Attributes.builder()
                .putRaw(ArtemisKinds.AUTO_CREATE_ADDRESS, true)
                .build();

        assertThat(QueueConfigs.toConfig("q", asked, true)).containsEntry("auto-create-address", true);
        assertThatThrownBy(() -> QueueConfigs.toConfig("q", asked, false)).isInstanceOf(BrokerException.class);
    }

    @Test
    void onlyWhatTheBrokerReportsIsComparedWithAnExistingQueue() {
        Map<String, Object> asked = Map.of("auto-create-address", true, "max-consumers", 5);

        assertThat(QueueConfigs.differences(asked, Map.of("max-consumers", 5))).isEmpty();
        assertThat(QueueConfigs.differences(asked, Map.of("max-consumers", 6))).containsExactly("max-consumers");
    }
}
