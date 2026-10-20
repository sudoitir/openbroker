package io.github.sudoitir.openbroker.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageFilter;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.MessageRef;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Shard;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class PortableFilterTest {

    private static final Message ORDER = new Message(
            new MessageRef(ResourceId.of("x.queue", "q"), Shard.node(NodeId.of("n")), "1"),
            MessageHeaders.builder().type("order.created").correlationId("c-1").build(),
            Map.of("region", PropertyValue.of("eu-west"), "attempts", PropertyValue.of(7)),
            MessageBody.empty(),
            Set.of(),
            Map.of());

    private static boolean matches(String expression) {
        return PortableFilter.parse(MessageFilter.portable(expression)).test(ORDER);
    }

    @Test
    void matchesExactPrefixAndSuffix() {
        assertThat(matches("header.type = 'order.created'")).isTrue();
        assertThat(matches("property.region ^= 'eu-'")).isTrue();
        assertThat(matches("property.region $= '-west'")).isTrue();
        assertThat(matches("property.region = 'eu'")).isFalse();
    }

    @Test
    void requiresEveryClauseAndAMissingFieldNeverMatches() {
        assertThat(matches("header.type = 'order.created' AND property.region ^= 'eu'"))
                .isTrue();
        assertThat(matches("header.type = 'order.created' AND property.missing = 'x'"))
                .isFalse();
    }

    @Test
    void comparesTypedPropertiesByTheirText() {
        assertThat(matches("property.attempts = '7'")).isTrue();
        assertThat(PortableFilter.parse(MessageFilter.portable("header.correlationId = 'c-1'"))
                        .test(ORDER))
                .isTrue();
    }

    @Test
    void readsADoubledQuoteAsOne() {
        Message m = new Message(
                ORDER.ref(),
                MessageHeaders.empty(),
                Map.of("name", PropertyValue.of("o'brien")),
                MessageBody.empty(),
                Set.of(),
                Map.of());
        assertThat(PortableFilter.parse(MessageFilter.portable("property.name = 'o''brien'"))
                        .test(m))
                .isTrue();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "",
                "type = 'x'",
                "header.type == 'x'",
                "header.type = x",
                "header.type = 'x' OR header.id = 'y'",
                "header.type = 'x",
                "header.nope = 'x'"
            })
    void refusesWhatDoesNotParse(String expression) {
        assertThatThrownBy(() -> matches(expression))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.reason()).isEqualTo(ErrorReasons.INVALID_FILTER));
    }

    @Test
    void refusesAnotherDialect() {
        assertThatThrownBy(() -> PortableFilter.parse(new MessageFilter("artemis.core-filter", "a=1")))
                .isInstanceOf(BrokerException.class);
    }
}
