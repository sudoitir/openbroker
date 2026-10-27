package io.github.sudoitir.openbroker.kafka.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Blob;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.MessagePart;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;

class RecordCodecTest {

    private static final ResourceId SOURCE = ResourceId.of("kafka.topic", "orders");

    /** What a consumer would read of the record a producer would send. */
    private static Message roundTrip(OutboundMessage out, long maxBody) {
        ProducerRecord<byte[], byte[]> sent = RecordCodec.record("orders", Optional.empty(), Optional.empty(), out);
        ConsumerRecord<byte[], byte[]> read = new ConsumerRecord<>("orders", 2, 7, sent.key(), sent.value());
        sent.headers().forEach(h -> read.headers().add(new RecordHeader(h.key(), h.value())));
        return RecordCodec.message(SOURCE, read, maxBody);
    }

    @Test
    void headersPropertiesAndBodyComeBackAsSent() {
        OutboundMessage out = new OutboundMessage(
                MessageHeaders.builder()
                        .id("m-1")
                        .correlationId("c-1")
                        .replyTo("replies")
                        .contentType("text/plain")
                        .type("order.placed")
                        .deduplicationId("d-1")
                        .origin("orders")
                        .originId("42")
                        .build(),
                Map.of(
                        "text", PropertyValue.of("hello"),
                        "count", PropertyValue.of(42L),
                        "small", new PropertyValue.OfInt(7),
                        "flag", PropertyValue.of(true),
                        "ratio", PropertyValue.of(0.5),
                        "raw", new PropertyValue.OfBytes(Blob.of(new byte[] {0, (byte) 0xFF, 1}))),
                MessageBody.of("body".getBytes(StandardCharsets.UTF_8)),
                Map.of());

        Message m = roundTrip(out, 1024);

        assertThat(m.headers().id()).contains("m-1");
        assertThat(m.headers().correlationId()).contains("c-1");
        assertThat(m.headers().replyTo()).contains("replies");
        assertThat(m.headers().contentType()).contains("text/plain");
        assertThat(m.headers().type()).contains("order.placed");
        assertThat(m.headers().deduplicationId()).contains("d-1");
        assertThat(m.headers().origin()).contains("orders");
        assertThat(m.headers().originId()).contains("42");
        assertThat(m.properties())
                .containsEntry("text", PropertyValue.of("hello"))
                .containsEntry("count", PropertyValue.of(42L))
                .containsEntry("small", new PropertyValue.OfInt(7))
                .containsEntry("flag", PropertyValue.of(true))
                .containsEntry("ratio", PropertyValue.of(0.5))
                .containsEntry("raw", new PropertyValue.OfBytes(Blob.of(new byte[] {0, (byte) 0xFF, 1})));
        assertThat(m.body().bytes().orElseThrow().asUtf8()).isEqualTo("body");
        assertThat(m.ref().position()).isEqualTo("7");
    }

    @Test
    void theCodecsOwnHeadersAreNotProperties() {
        Message m = roundTrip(
                new OutboundMessage(
                        MessageHeaders.builder().correlationId("c").build(),
                        Map.of("count", PropertyValue.of(1L)),
                        MessageBody.empty(),
                        Map.of()),
                10);

        assertThat(m.properties()).containsOnlyKeys("count");
    }

    @Test
    void aRecordAnotherApplicationWroteShowsItsHeadersAsStringsOrBytes() {
        ConsumerRecord<byte[], byte[]> foreign =
                new ConsumerRecord<>("orders", 0, 0, null, "v".getBytes(StandardCharsets.UTF_8));
        foreign.headers().add(new RecordHeader("trace", "abc".getBytes(StandardCharsets.UTF_8)));
        foreign.headers().add(new RecordHeader("blob", new byte[] {(byte) 0xC3, 0x28}));

        Message m = RecordCodec.message(SOURCE, foreign, 10);

        assertThat(m.properties())
                .containsEntry("trace", PropertyValue.of("abc"))
                .containsEntry("blob", new PropertyValue.OfBytes(Blob.of(new byte[] {(byte) 0xC3, 0x28})));
    }

    @Test
    void aBodyPastTheBoundIsCutAndSaysSo() {
        Message m = roundTrip(OutboundMessage.of(MessageBody.of(new byte[100])), 10);

        assertThat(m.body().size()).isEqualTo(10);
        assertThat(m.truncated()).containsExactly(MessagePart.BODY);
    }

    @Test
    void aMissingValueIsATombstone() {
        ConsumerRecord<byte[], byte[]> tombstone =
                new ConsumerRecord<>("orders", 0, 0, "k".getBytes(StandardCharsets.UTF_8), null);

        Message m = RecordCodec.message(SOURCE, tombstone, 10);

        assertThat(m.body().isTombstone()).isTrue();
        assertThat(m.headers().key().orElseThrow().asUtf8()).isEqualTo("k");
    }

    @Test
    void theKeyIsTheMessagesKeyThenItsGroupThenTheRoutingKey() {
        OutboundMessage keyed = new OutboundMessage(
                MessageHeaders.builder().key(Blob.utf8("k")).groupId("g").build(),
                Map.of(),
                MessageBody.empty(),
                Map.of());
        OutboundMessage grouped = new OutboundMessage(
                MessageHeaders.builder().groupId("g").build(), Map.of(), MessageBody.empty(), Map.of());

        assertThat(RecordCodec.record("t", Optional.empty(), Optional.of("r"), keyed)
                        .key())
                .isEqualTo("k".getBytes(StandardCharsets.UTF_8));
        assertThat(RecordCodec.record("t", Optional.empty(), Optional.of("r"), grouped)
                        .key())
                .isEqualTo("g".getBytes(StandardCharsets.UTF_8));
        assertThat(RecordCodec.record("t", Optional.empty(), Optional.of("r"), OutboundMessage.of(MessageBody.empty()))
                        .key())
                .isEqualTo("r".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void whatAKafkaRecordCannotHoldIsRefusedNotDropped() {
        for (MessageHeaders unsupported : new MessageHeaders[] {
            MessageHeaders.builder().priority(5).build(),
            MessageHeaders.builder().expiresAt(Instant.now()).build(),
            MessageHeaders.builder().deliverAt(Instant.now()).build()
        }) {
            assertThatThrownBy(() -> RecordCodec.record(
                            "t",
                            Optional.empty(),
                            Optional.empty(),
                            new OutboundMessage(unsupported, Map.of(), MessageBody.empty(), Map.of())))
                    .isInstanceOfSatisfying(
                            BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.UNIMPLEMENTED));
        }
    }

    @Test
    void aPropertyCannotTakeTheNameOfAReservedHeader() {
        assertThatThrownBy(() -> RecordCodec.record(
                        "t",
                        Optional.empty(),
                        Optional.empty(),
                        new OutboundMessage(
                                MessageHeaders.empty(),
                                Map.of("openbroker.id", PropertyValue.of("x")),
                                MessageBody.empty(),
                                Map.of())))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT));
    }

    @Test
    void theProducersTimestampIsTheProducedAt() {
        Instant at = Instant.parse("2026-01-02T03:04:05Z");

        ProducerRecord<byte[], byte[]> record = RecordCodec.record(
                "t",
                Optional.empty(),
                Optional.empty(),
                new OutboundMessage(
                        MessageHeaders.builder().producedAt(at).build(), Map.of(), MessageBody.empty(), Map.of()));

        assertThat(record.timestamp()).isEqualTo(at.toEpochMilli());
    }
}
