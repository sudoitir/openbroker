package io.github.sudoitir.openbroker.artemis.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.artemis.ArtemisKinds;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.MessageRef;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.Shard;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** The copies a receiver sends instead of a message: what they keep, what they drop, what they add. */
class CopiesTest {

    private static final Instant NOW = Instant.parse("2026-10-21T10:00:00Z");

    private static Message message(int deliveryCount, Map<String, PropertyValue> properties) {
        return new Message(
                new MessageRef(
                        new ResourceId(ArtemisKinds.QUEUE, ResourcePath.of("orders")),
                        Shard.node(NodeId.of("n1")),
                        "42"),
                MessageHeaders.builder()
                        .id("ID:original")
                        .correlationId("corr-1")
                        .groupId("g1")
                        .deduplicationId("dedupe-1")
                        .deliveryCount(deliveryCount)
                        .deliverAt(NOW.minusSeconds(60))
                        .build(),
                properties,
                MessageBody.of("hello".getBytes(StandardCharsets.UTF_8)),
                Set.of(),
                Map.of());
    }

    @Test
    void theAttemptIsTheBrokersCountUntilACopyCarriesALaterOne() {
        Message plain = message(1, Map.of());
        Message copy = message(1, Map.of(Copies.ATTEMPT, PropertyValue.of(3)));

        assertThat(Copies.attempt(plain, 2)).isEqualTo(2);
        assertThat(Copies.attempt(copy, 1)).isEqualTo(3);
        assertThat(Copies.attempt(copy, 5)).isEqualTo(5);
        assertThat(Copies.withAttempt(copy, 1).headers().deliveryCount()).hasValue(3);
    }

    @Test
    void aDelayedCopyIsANewMessageDueAfterTheDelayOnTheNextAttempt() {
        OutboundMessage copy =
                Copies.delayed(message(2, Map.of("seq", PropertyValue.of(7))), Duration.ofSeconds(5), true, NOW);

        MessageHeaders headers = copy.headers();
        assertThat(headers.deliverAt()).hasValue(NOW.plusSeconds(5));
        assertThat(copy.properties())
                .containsEntry(Copies.ATTEMPT, PropertyValue.of(3))
                .containsEntry("seq", PropertyValue.of(7));
        assertThat(headers.deduplicationId())
                .as("the broker would drop a copy that repeats the original's dedupe id")
                .isEmpty();
        assertThat(headers.id()).as("the copy is a new message").isEmpty();
        assertThat(headers.deliveryCount()).isEmpty();
        assertThat(headers.groupId()).hasValue("g1");
        assertThat(headers.correlationId()).hasValue("corr-1");
        assertThat(copy.body().size()).isEqualTo(5);
    }

    @Test
    void aReleaseThatIsNotAFailedDeliveryStaysOnTheSameAttempt() {
        OutboundMessage copy = Copies.delayed(message(2, Map.of()), Duration.ofSeconds(1), false, NOW);

        assertThat(copy.properties()).containsEntry(Copies.ATTEMPT, PropertyValue.of(2));
    }

    @Test
    void aDeadLetterCopyCarriesWhyAndIsDueAtOnce() {
        OutboundMessage copy = Copies.deadLetter(message(4, Map.of()), "poison");

        assertThat(copy.properties()).containsEntry(Copies.DEAD_LETTER_REASON, PropertyValue.of("poison"));
        assertThat(copy.headers().deliverAt()).isEmpty();
        assertThat(copy.headers().deduplicationId()).isEmpty();
        assertThat(copy.headers().id()).isEmpty();
        assertThat(copy.headers().groupId()).hasValue("g1");
    }
}
