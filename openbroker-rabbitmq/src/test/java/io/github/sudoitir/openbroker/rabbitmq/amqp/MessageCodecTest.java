package io.github.sudoitir.openbroker.rabbitmq.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.impl.LongStringHelper;
import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Blob;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.MessagePart;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.ResourceId;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MessageCodecTest {

    private static final Instant NOW = Instant.parse("2026-03-04T05:06:07Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    private static final ResourceId QUEUE = ResourceId.of("rabbitmq.quorum-queue", "/", "orders");

    private static OutboundMessage outbound(MessageHeaders headers) {
        return new OutboundMessage(headers, Map.of(), MessageBody.of("hello".getBytes()), Map.of());
    }

    private static BrokerException refused(OutboundMessage message) {
        try {
            MessageCodec.properties(message, CLOCK);
        } catch (BrokerException e) {
            return e;
        }
        throw new AssertionError("expected a refusal");
    }

    @Test
    void headersBecomeProperties() {
        MessageHeaders headers = MessageHeaders.builder()
                .id("m-1")
                .correlationId("c-1")
                .replyTo("replies")
                .contentType("text/plain")
                .type("order")
                .priority(5)
                .durable(true)
                .producedAt(NOW.minusSeconds(60))
                .expiresAt(NOW.plusSeconds(30))
                .build();

        AMQP.BasicProperties props = MessageCodec.properties(outbound(headers), CLOCK);

        assertThat(props.getMessageId()).isEqualTo("m-1");
        assertThat(props.getCorrelationId()).isEqualTo("c-1");
        assertThat(props.getReplyTo()).isEqualTo("replies");
        assertThat(props.getContentType()).isEqualTo("text/plain");
        assertThat(props.getType()).isEqualTo("order");
        assertThat(props.getPriority()).isEqualTo(5);
        assertThat(props.getDeliveryMode()).isEqualTo(2);
        assertThat(props.getTimestamp()).isEqualTo(Date.from(NOW.minusSeconds(60)));
        assertThat(props.getExpiration()).isEqualTo("30000");
    }

    @Test
    void aMessageWithoutTimeIsDatedFromTheClock() {
        AMQP.BasicProperties props = MessageCodec.properties(outbound(MessageHeaders.empty()), CLOCK);

        assertThat(props.getTimestamp()).isEqualTo(Date.from(NOW));
        assertThat(props.getDeliveryMode()).isNull();
        assertThat(props.getHeaders()).isNull();
        assertThat(MessageCodec.properties(outbound(MessageHeaders.empty())).getTimestamp())
                .isNotNull();
    }

    @Test
    void nonDurableIsTransient() {
        assertThat(MessageCodec.properties(
                                outbound(MessageHeaders.builder().durable(false).build()), CLOCK)
                        .getDeliveryMode())
                .isEqualTo(1);
    }

    @Test
    void whatRabbitMqHasNoPlaceForIsRefused() {
        for (MessageHeaders headers : List.of(
                MessageHeaders.builder().key(Blob.utf8("k")).build(),
                MessageHeaders.builder().groupId("g").build(),
                MessageHeaders.builder().deduplicationId("d").build(),
                MessageHeaders.builder().deliverAt(NOW.plusSeconds(5)).build(),
                MessageHeaders.builder().priority(256).build(),
                MessageHeaders.builder().priority(-1).build(),
                MessageHeaders.builder().expiresAt(NOW.minusSeconds(1)).build())) {
            assertThat(refused(outbound(headers)).status()).isEqualTo(Status.INVALID_ARGUMENT);
        }
        assertThat(refused(outbound(MessageHeaders.builder().groupId("g").build())))
                .hasMessageContaining("takes no group id");
    }

    @Test
    void propertiesAndTheExtensionBecomeHeaders() {
        Attributes extension = Attributes.builder()
                .putRaw("content-encoding", "gzip")
                .putRaw("user-id", "ops")
                .putRaw("app-id", "app")
                .putRaw("expiration", "9000")
                .putRaw("header.when", NOW)
                .putRaw("header.blob", Blob.utf8("x"))
                .putRaw("header.nested", Map.of("at", NOW, "list", List.of(Blob.utf8("y"))))
                .putRaw("exchange", "ignored")
                .build();
        OutboundMessage message = new OutboundMessage(
                MessageHeaders.empty(),
                Map.of(
                        "s", PropertyValue.of("text"),
                        "b", PropertyValue.of(true),
                        "bytes", new PropertyValue.OfBytes(Blob.utf8("z")),
                        "i", PropertyValue.of(3)),
                MessageBody.empty(),
                Map.of("rabbitmq", extension));

        AMQP.BasicProperties props = MessageCodec.properties(message, CLOCK);

        assertThat(props.getContentEncoding()).isEqualTo("gzip");
        assertThat(props.getUserId()).isEqualTo("ops");
        assertThat(props.getAppId()).isEqualTo("app");
        assertThat(props.getExpiration()).isEqualTo("9000");
        Map<String, Object> headers = props.getHeaders();
        assertThat(headers).containsEntry("s", "text").containsEntry("b", true).containsEntry("i", 3);
        assertThat((byte[]) headers.get("bytes")).containsExactly('z');
        assertThat((byte[]) headers.get("blob")).containsExactly('x');
        assertThat(headers.get("when")).isEqualTo(Date.from(NOW));
        @SuppressWarnings("unchecked")
        Map<String, Object> nested = (Map<String, Object>) headers.get("nested");
        assertThat(nested.get("at")).isEqualTo(Date.from(NOW));
        assertThat(((List<?>) nested.get("list")).get(0)).isInstanceOf(byte[].class);
    }

    @Test
    void theBodyIsTakenAsItIs() throws IOException {
        assertThat(MessageCodec.body(outbound(MessageHeaders.empty()))).isEqualTo("hello".getBytes());
        MessageBody streamed = MessageBody.streamed(3, () -> new java.io.ByteArrayInputStream(new byte[] {1, 2, 3}));
        assertThat(MessageCodec.body(new OutboundMessage(MessageHeaders.empty(), Map.of(), streamed, Map.of())))
                .containsExactly(1, 2, 3);
        MessageBody broken = MessageBody.streamed(1, () -> {
            throw new IOException("gone");
        });
        assertThatThrownBy(() ->
                        MessageCodec.body(new OutboundMessage(MessageHeaders.empty(), Map.of(), broken, Map.of())))
                .isInstanceOf(BrokerException.class);
        assertThatThrownBy(() -> MessageCodec.body(
                        new OutboundMessage(MessageHeaders.empty(), Map.of(), MessageBody.tombstone(), Map.of())))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT));
    }

    @Test
    void aDeliveryKeepsTheTagOutOfTheMessage() {
        Map<String, Object> headers = new java.util.HashMap<>();
        headers.put("name", LongStringHelper.asLongString("v"));
        headers.put("n", 4L);
        headers.put("flag", false);
        headers.put("raw", new byte[] {9});
        headers.put("x-delivery-count", 2L);
        headers.put("x-stream-offset", 77L);
        headers.put("when", Date.from(NOW));
        headers.put("price", new BigDecimal("1.5"));
        headers.put("list", List.of(LongStringHelper.asLongString("a")));
        headers.put("nothing", null);
        headers.put("short", (short) 1);
        headers.put("byte", (byte) 2);
        headers.put("float", 1.5f);
        headers.put("double", 2.5d);
        headers.put("int", 3);
        headers.put("string", "plain");
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .messageId("m-9")
                .correlationId("c")
                .replyTo("r")
                .contentType("application/json")
                .contentEncoding("gzip")
                .type("t")
                .priority(3)
                .deliveryMode(2)
                .timestamp(Date.from(NOW))
                .expiration("1000")
                .userId("u")
                .appId("a")
                .headers(headers)
                .build();

        Message message = MessageCodec.message(QUEUE, new Envelope(12, true, "ex", "rk"), props, "body".getBytes());

        assertThat(message.ref().source()).isEqualTo(QUEUE);
        assertThat(message.ref().position()).isEqualTo("77");
        assertThat(message.headers().id()).contains("m-9");
        assertThat(message.headers().correlationId()).contains("c");
        assertThat(message.headers().replyTo()).contains("r");
        assertThat(message.headers().contentType()).contains("application/json");
        assertThat(message.headers().type()).contains("t");
        assertThat(message.headers().priority()).hasValue(3);
        assertThat(message.headers().durable()).contains(true);
        assertThat(message.headers().producedAt()).contains(NOW);
        assertThat(message.headers().deliveryCount()).hasValue(3);
        assertThat(message.properties())
                .containsEntry("name", PropertyValue.of("v"))
                .containsEntry("n", PropertyValue.of(4L))
                .containsEntry("flag", PropertyValue.of(false))
                .containsEntry("short", new PropertyValue.OfShort((short) 1))
                .containsEntry("byte", new PropertyValue.OfByte((byte) 2))
                .containsEntry("float", new PropertyValue.OfFloat(1.5f))
                .containsEntry("double", PropertyValue.of(2.5d))
                .containsEntry("int", PropertyValue.of(3))
                .containsEntry("string", PropertyValue.of("plain"))
                .containsEntry("raw", new PropertyValue.OfBytes(Blob.of(new byte[] {9})))
                .doesNotContainKeys("x-delivery-count", "x-stream-offset", "when", "nothing");
        Attributes extension = message.extension("rabbitmq");
        assertThat(extension.raw("exchange")).contains("ex");
        assertThat(extension.raw("routing_key")).contains("rk");
        assertThat(extension.raw("redelivered")).contains(true);
        assertThat(extension.raw("x-stream-offset")).contains(77L);
        assertThat(extension.raw("content-encoding")).contains("gzip");
        assertThat(extension.raw("expiration")).contains("1000");
        assertThat(extension.raw("user-id")).contains("u");
        assertThat(extension.raw("app-id")).contains("a");
        assertThat(extension.raw("header.when")).contains(NOW);
        assertThat(extension.raw("header.price")).contains(new BigDecimal("1.5"));
        assertThat(extension.raw("header.list")).contains(List.of("a"));
        assertThat(extension.asMap()).doesNotContainKey("delivery_tag");
        assertThat(message.body().bytes()).contains(Blob.utf8("body"));
        assertThat(message.truncated()).isEmpty();
    }

    @Test
    void aDeliveryWithoutAnOffsetCarriesItsTagInTheReference() {
        Message message = MessageCodec.message(
                QUEUE, new Envelope(12, false, "", "orders"), new AMQP.BasicProperties(), new byte[0]);

        assertThat(message.ref().position()).isEqualTo("12");
        assertThat(message.headers().deliveryCount()).isEmpty();
        assertThat(message.headers().durable()).isEmpty();
        assertThat(message.properties()).isEmpty();
        assertThat(message.extension("rabbitmq").raw("redelivered")).contains(false);
        assertThat(message.extension("rabbitmq").raw("exchange")).isEmpty();
    }

    @Test
    void theFirstDeathNamesTheOrigin() {
        Map<String, Object> death = Map.of(
                "queue", LongStringHelper.asLongString("source"),
                "exchange", LongStringHelper.asLongString("ex"),
                "count", 1L,
                "reason", LongStringHelper.asLongString("rejected"),
                "time", Date.from(NOW),
                "routing-keys", List.of(LongStringHelper.asLongString("rk")));
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .messageId("orig")
                .headers(Map.of("x-death", List.of(death)))
                .build();

        Message message = MessageCodec.message(QUEUE, new Envelope(1, false, "", "q"), props, new byte[0]);

        assertThat(message.headers().origin()).contains("source");
        assertThat(message.headers().originId()).contains("orig");
        assertThat(message.extension("rabbitmq").raw("header.x-death")).isPresent();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> deaths = (List<Map<String, Object>>)
                message.extension("rabbitmq").raw("header.x-death").orElseThrow();
        assertThat(deaths.get(0))
                .containsEntry("queue", "source")
                .containsEntry("time", NOW)
                .containsEntry("count", 1L);
    }

    @Test
    void aDeathWithOnlyAnExchangeNamesTheExchange() {
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .headers(Map.of("x-death", List.of(Map.of("exchange", "ex"))))
                .build();

        Message message = MessageCodec.message(QUEUE, new Envelope(1, false, "", "q"), props, new byte[0]);

        assertThat(message.headers().origin()).contains("ex");
    }

    @Test
    void aLongBodyIsCutAndSaysSo() {
        Message message = MessageCodec.message(
                QUEUE, new Envelope(1, false, "", "q"), new AMQP.BasicProperties(), "abcdef".getBytes(), 4);

        assertThat(message.body().bytes()).contains(Blob.utf8("abcd"));
        assertThat(message.truncated()).containsExactly(MessagePart.BODY);
    }

    @Test
    void aReceivedMessageSurvivesBeingSentAgain() {
        Map<String, Object> headers = Map.of(
                "name", LongStringHelper.asLongString("v"),
                "when", Date.from(NOW),
                "table", Map.of("k", LongStringHelper.asLongString("w")));
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .messageId("m")
                .contentType("text/plain")
                .contentEncoding("gzip")
                .deliveryMode(2)
                .priority(1)
                .timestamp(Date.from(NOW))
                .headers(headers)
                .build();
        Message received = MessageCodec.message(QUEUE, new Envelope(1, false, "", "q"), props, "x".getBytes());

        AMQP.BasicProperties again = MessageCodec.properties(OutboundMessage.copyOf(received), CLOCK);

        assertThat(again.getMessageId()).isEqualTo("m");
        assertThat(again.getContentType()).isEqualTo("text/plain");
        assertThat(again.getContentEncoding()).isEqualTo("gzip");
        assertThat(again.getDeliveryMode()).isEqualTo(2);
        assertThat(again.getPriority()).isEqualTo(1);
        assertThat(again.getTimestamp()).isEqualTo(Date.from(NOW));
        assertThat(again.getHeaders()).containsEntry("name", "v").containsEntry("when", Date.from(NOW));
        assertThat(again.getHeaders().get("table")).isEqualTo(Map.of("k", "w"));
    }
}
