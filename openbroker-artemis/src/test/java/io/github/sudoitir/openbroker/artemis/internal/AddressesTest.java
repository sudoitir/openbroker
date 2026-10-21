package io.github.sudoitir.openbroker.artemis.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

/** The dedupe window an address reports, with the broker's default when its settings cannot be read. */
class AddressesTest {

    @Test
    void anAddressesCacheSizeIsItsWindowAndSurvivesARestartWhenTheBrokerPersistsIt() {
        Addresses.Settings settings =
                new Addresses.Settings(OptionalLong.of(500), OptionalInt.of(10), Optional.of("DLQ"), true);

        assertThat(settings.window().ids()).hasValue(500);
        assertThat(settings.window().time()).isEmpty();
        assertThat(settings.window().durable()).isTrue();
    }

    @Test
    void anUnreadableCacheSizeIsTheBrokersDefault() {
        Addresses.Settings settings =
                new Addresses.Settings(OptionalLong.empty(), OptionalInt.empty(), Optional.empty(), false);

        assertThat(settings.window().ids()).hasValue(Addresses.DEFAULT_ID_CACHE_SIZE);
        assertThat(settings.window().durable()).isFalse();
    }

    @Test
    void theAddressOfAQueueIsTheFirstPartOfItsFullyQualifiedName() {
        assertThat(Targets.addressOf("orders::orders.worker")).isEqualTo("orders");
        assertThat(Targets.addressOf("orders")).isEqualTo("orders");
    }
}
