package io.github.sudoitir.openbroker.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ResourceIdTest {

    @Test
    void formatsUnreservedCharactersAsTheyAre() {
        assertThat(ResourceId.of("artemis.queue", "orders.eu-1_x~").format()).isEqualTo("artemis.queue/orders.eu-1_x~");
    }

    @Test
    void encodesEveryOtherByteInUpperCaseHex() {
        assertThat(ResourceId.of("artemis.queue", "a/b%c d").format()).isEqualTo("artemis.queue/a%2Fb%25c%20d");
        assertThat(ResourceId.of("rabbitmq.queue", "vhost", "ü").format()).isEqualTo("rabbitmq.queue/vhost/%C3%BC");
    }

    @ParameterizedTest
    @ValueSource(strings = {"orders", "a/b", "100%", " lead", "x::y", "ü", "%2F", "a b/c%d"})
    void roundTripsFromTheIdToTheTextAndBack(String segment) {
        ResourceId id = ResourceId.of("artemis.queue", "ns", segment);
        assertThat(ResourceId.parse(id.format())).isEqualTo(id);
    }

    @ParameterizedTest
    @ValueSource(strings = {"artemis.queue/orders", "artemis.queue/a%2Fb", "kafka.group-topic/g1/orders%20eu"})
    void roundTripsFromTheTextToTheIdAndBack(String text) {
        assertThat(ResourceId.parse(text).format()).isEqualTo(text);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "artemis.queue",
                "artemis.queue/",
                "artemis.queue/a//b",
                "artemis.queue/%41",
                "artemis.queue/a%2fb",
                "artemis.queue/100%",
                "artemis.queue/%G1",
                "artemis.queue/a b",
                "Artemis.queue/a",
                "queue/a"
            })
    void rejectsTextThatIsNotCanonical(String text) {
        assertThatIllegalArgumentException().isThrownBy(() -> ResourceId.parse(text));
    }

    @Test
    void namesTheSystemOfItsKind() {
        assertThat(KindId.of("kafka.group-topic").system()).isEqualTo("kafka");
    }
}
