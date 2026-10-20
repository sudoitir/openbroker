package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.messaging.Blob;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.MessageBody;
import io.github.sudoitir.openbroker.messaging.MessageHeaders;
import io.github.sudoitir.openbroker.messaging.MessagePart;
import io.github.sudoitir.openbroker.messaging.MessageRef;
import io.github.sudoitir.openbroker.messaging.OutboundMessage;
import io.github.sudoitir.openbroker.messaging.PropertyValue;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Shard;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.jspecify.annotations.Nullable;

/**
 * Core messages and the SDK's. A body is the payload: a text message's text as UTF-8, any other
 * message's bytes verbatim; an object body is never deserialized. Large bodies stream through a
 * spool file. A message read and sent again within Artemis keeps its type, its JMS message id and
 * every property but the broker's own bookkeeping, which describes where it was and never travels.
 *
 * <p>Artemis stores a text body as a marker byte, the byte length as a big-endian int, then the
 * text as UTF-16LE; it is converted to and from UTF-8 as it streams, so a large text never sits in
 * memory.
 */
final class CoreBodies {

    static final byte DEFAULT = 0;
    static final byte TEXT = 3;
    static final byte BYTES = 4;

    /** Extension keys under the {@code artemis} namespace. */
    static final String MESSAGE_TYPE = BrowseRows.MESSAGE_TYPE;

    static final String LARGE = "artemis.large";
    static final String ADDRESS = BrowseRows.ADDRESS;

    private static final SimpleString CORRELATION_ID = SimpleString.of(BrowseRows.CORRELATION_ID);
    private static final SimpleString TYPE = SimpleString.of(BrowseRows.TYPE);
    private static final SimpleString REPLY_TO = SimpleString.of(BrowseRows.REPLY_TO);
    private static final SimpleString GROUP_ID = SimpleString.of(BrowseRows.GROUP_ID);
    private static final SimpleString DUPLICATE_ID = SimpleString.of(BrowseRows.DUPLICATE_ID);
    private static final SimpleString SCHEDULED_DELIVERY = SimpleString.of(BrowseRows.SCHEDULED_DELIVERY);
    private static final SimpleString LARGE_BODY_SIZE = SimpleString.of("_AMQ_LARGE_SIZE");

    private static final Set<SimpleString> HEADERS =
            Set.of(CORRELATION_ID, TYPE, REPLY_TO, GROUP_ID, DUPLICATE_ID, SCHEDULED_DELIVERY);

    /** The broker's bookkeeping about where a message was; never copied to a message sent anew. */
    static final Set<String> BOOKKEEPING = Set.of(
            "_AMQ_ORIG_ADDRESS",
            "_AMQ_ORIG_QUEUE",
            "_AMQ_ORIG_MESSAGE_ID",
            "_AMQ_ORIG_ROUTING_TYPE",
            "_AMQ_ROUTE_TO",
            "_AMQ_ACK_ROUTE_TO",
            "_AMQ_SCALEDOWN_TO",
            "_AMQ_BRIDGE_DUP",
            "_AMQ_ROUTING_TYPE",
            "_AMQ_ACTUAL_EXPIRY",
            "_AMQ_LARGE_SIZE",
            "_AMQ_LARGE_COMPRESSED");

    private CoreBodies() {}

    /**
     * One received message.
     *
     * @param spool where a large body is saved, or empty to read at most {@code maxBodyBytes} of it
     */
    static Message read(ClientMessage m, ResourceId subscription, NodeId node, long maxBodyBytes, Optional<Spool> spool)
            throws ActiveMQException, IOException {
        Map<String, PropertyValue> properties = new LinkedHashMap<>();
        for (SimpleString name : m.getPropertyNames()) {
            if (!HEADERS.contains(name)) {
                PropertyValue value = typed(m.getObjectProperty(name));
                if (value != null) {
                    properties.put(name.toString(), value);
                }
            }
        }
        MessageHeaders.Builder headers = MessageHeaders.builder()
                .correlationId(text(m.getObjectProperty(CORRELATION_ID)))
                .type(text(m.getObjectProperty(TYPE)))
                .replyTo(text(m.getObjectProperty(REPLY_TO)))
                .groupId(text(m.getObjectProperty(GROUP_ID)))
                .deduplicationId(text(m.getObjectProperty(DUPLICATE_ID)))
                .priority((int) m.getPriority())
                .durable(m.isDurable())
                .deliveryCount(m.getDeliveryCount());
        Object userId = m.getUserID();
        if (userId != null) {
            headers.id("ID:" + userId);
        }
        if (m.getTimestamp() > 0) {
            headers.producedAt(Instant.ofEpochMilli(m.getTimestamp()));
        }
        if (m.getExpiration() > 0) {
            headers.expiresAt(Instant.ofEpochMilli(m.getExpiration()));
        }
        if (m.getObjectProperty(SCHEDULED_DELIVERY) instanceof Number at) {
            headers.deliverAt(Instant.ofEpochMilli(at.longValue()));
        }

        Set<MessagePart> truncated = EnumSet.noneOf(MessagePart.class);
        boolean text = m.getType() == TEXT;
        MessageBody body;
        if (m.isLargeMessage() && spool.isPresent()) {
            Path file = spool.get().file(largeSize(m));
            try (OutputStream out = Files.newOutputStream(file)) {
                save(m, out, text, Long.MAX_VALUE);
            }
            long size = Files.size(file);
            body = MessageBody.streamed(size, () -> Files.newInputStream(file));
        } else if (m.isLargeMessage()) {
            CappedOutput out = new CappedOutput(maxBodyBytes);
            save(m, out, text, maxBodyBytes);
            if (out.cut()) {
                truncated.add(MessagePart.BODY);
            }
            body = MessageBody.of(Blob.of(out.toByteArray()));
        } else {
            ActiveMQBuffer buffer = m.getReadOnlyBodyBuffer();
            byte[] bytes;
            if (text) {
                SimpleString s = buffer.readNullableSimpleString();
                bytes = s == null ? new byte[0] : s.toString().getBytes(StandardCharsets.UTF_8);
            } else {
                bytes = new byte[buffer.readableBytes()];
                buffer.readBytes(bytes);
            }
            if (bytes.length > maxBodyBytes) {
                byte[] cut = new byte[(int) maxBodyBytes];
                System.arraycopy(bytes, 0, cut, 0, cut.length);
                bytes = cut;
                truncated.add(MessagePart.BODY);
            }
            body = MessageBody.of(Blob.of(bytes));
        }
        Attributes.Builder artemis =
                Attributes.builder().putRaw(MESSAGE_TYPE, (int) m.getType()).putRaw(LARGE, m.isLargeMessage());
        if (m.getAddress() != null) {
            artemis.putRaw(ADDRESS, m.getAddress());
        }
        return new Message(
                new MessageRef(subscription, Shard.node(node), String.valueOf(m.getMessageID())),
                headers.build(),
                properties,
                body,
                truncated,
                Map.of("artemis", artemis.build()));
    }

    /**
     * The Core message for an outbound one. A streamed body is sent as a large message; a streamed
     * text is first re-encoded into a spool file, whose deletion the returned handle owns.
     */
    static Built write(ClientSession session, OutboundMessage out, Clock clock, Optional<Spool> spool)
            throws ActiveMQException, IOException {
        MessageHeaders h = out.headers();
        if (h.key().isPresent()) {
            throw Queries.invalid("An Artemis message takes no key.");
        }
        Optional<byte[]> bytes = out.body().bytes().map(Blob::toByteArray);
        byte type = type(out, bytes);
        long expiration = h.expiresAt().map(Instant::toEpochMilli).orElse(0L);
        long timestamp = h.producedAt().map(Instant::toEpochMilli).orElse(clock.millis());
        byte priority = (byte) (h.priority().isPresent() ? h.priority().getAsInt() : 4);
        ClientMessage m = session.createMessage(type, h.durable().orElse(true), expiration, timestamp, priority);
        if (h.id().isPresent()) {
            String id = h.id().get();
            try {
                m.setUserID(UUIDGenerator.getInstance()
                        .fromJavaUUID(UUID.fromString(id.startsWith("ID:") ? id.substring(3) : id)));
            } catch (IllegalArgumentException e) {
                throw Queries.invalid("An Artemis message id is a UUID, not " + id + ".");
            }
        }
        for (Map.Entry<String, PropertyValue> p : out.properties().entrySet()) {
            if (!BOOKKEEPING.contains(p.getKey())) {
                m.putObjectProperty(SimpleString.of(p.getKey()), coreValue(p.getValue()));
            }
        }
        h.correlationId().ifPresent(v -> m.putStringProperty(CORRELATION_ID, SimpleString.of(v)));
        h.type().ifPresent(v -> m.putStringProperty(TYPE, SimpleString.of(v)));
        h.replyTo().ifPresent(v -> m.putStringProperty(REPLY_TO, SimpleString.of(v)));
        h.groupId().ifPresent(v -> m.putStringProperty(GROUP_ID, SimpleString.of(v)));
        h.deduplicationId().ifPresent(v -> m.putStringProperty(DUPLICATE_ID, SimpleString.of(v)));
        h.deliverAt().ifPresent(v -> m.putLongProperty(SCHEDULED_DELIVERY, v.toEpochMilli()));

        if (bytes.isPresent()) {
            if (type == TEXT) {
                m.getBodyBuffer()
                        .writeNullableSimpleString(SimpleString.of(new String(bytes.get(), StandardCharsets.UTF_8)));
            } else {
                m.getBodyBuffer().writeBytes(bytes.get());
            }
            return new Built(m, null, null);
        }
        if (type != TEXT) {
            InputStream in = out.body().openStream();
            m.setBodyInputStream(in);
            return new Built(m, null, in);
        }
        Path file = spool.orElseThrow(() ->
                        Queries.invalid("A streamed text body is re-encoded through a spool directory; configure one."))
                .file(out.body().size() * 2 + 5);
        encodeText(out.body().openStream(), file);
        InputStream in = Files.newInputStream(file);
        m.setBodyInputStream(in);
        return new Built(m, file, in);
    }

    /** A built message and what its body holds open: close it once the send is settled. */
    record Built(
            ClientMessage message,
            @Nullable Path spool,
            @Nullable InputStream body) implements AutoCloseable {
        @Override
        public void close() throws IOException {
            if (body != null) {
                body.close();
            }
            if (spool != null) {
                Files.deleteIfExists(spool);
            }
        }
    }

    /** Where large bodies are saved; each file is the caller's to delete. */
    interface Spool {
        /** A new owner-only file with room for {@code size} bytes. */
        Path file(long size) throws IOException;
    }

    private static byte type(OutboundMessage out, Optional<byte[]> bytes) {
        Object declared = out.extensions()
                .getOrDefault("artemis", Attributes.empty())
                .raw(MESSAGE_TYPE)
                .orElse(null);
        if (declared instanceof Number n) {
            return n.byteValue();
        }
        boolean textual = out.headers()
                .contentType()
                .map(t -> t.startsWith("text/") || t.contains("json") || t.contains("xml"))
                .orElse(true);
        return textual && bytes.map(CoreBodies::isUtf8).orElse(false) ? TEXT : BYTES;
    }

    static boolean isUtf8(byte[] bytes) {
        try {
            StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes));
            return true;
        } catch (CharacterCodingException e) {
            return false;
        }
    }

    /** Streams a large body out, as UTF-8 when it is text. */
    private static void save(ClientMessage m, OutputStream out, boolean text, long limit)
            throws ActiveMQException, IOException {
        if (!text) {
            m.saveToOutputStream(out);
            return;
        }
        TextDecoder decoder = new TextDecoder(out, limit);
        m.saveToOutputStream(decoder);
        decoder.close();
    }

    private static long largeSize(ClientMessage m) {
        Object size = m.getObjectProperty(LARGE_BODY_SIZE);
        return size instanceof Number n ? n.longValue() : m.getBodySize();
    }

    private static @Nullable PropertyValue typed(@Nullable Object v) {
        return switch (v) {
            case null -> null;
            case Boolean b -> PropertyValue.of(b);
            case Byte b -> new PropertyValue.OfByte(b);
            case Short s -> new PropertyValue.OfShort(s);
            case Integer i -> PropertyValue.of(i);
            case Long l -> PropertyValue.of(l);
            case Float f -> new PropertyValue.OfFloat(f);
            case Double d -> PropertyValue.of(d);
            case byte[] bytes -> new PropertyValue.OfBytes(Blob.of(bytes));
            default -> PropertyValue.of(v.toString());
        };
    }

    private static Object coreValue(PropertyValue value) {
        return switch (value) {
            case PropertyValue.OfString s -> SimpleString.of(s.value());
            case PropertyValue.OfBytes b -> b.value().toByteArray();
            default -> value.boxed();
        };
    }

    private static @Nullable String text(@Nullable Object v) {
        return v == null ? null : v.toString();
    }

    /** Writes a UTF-8 text stream as Artemis encodes a text body, into {@code file}. */
    static void encodeText(InputStream utf8, Path file) throws IOException {
        long chars = 0;
        try (Reader in = new InputStreamReader(utf8, StandardCharsets.UTF_8);
                RandomAccessFile out = new RandomAccessFile(file.toFile(), "rw")) {
            out.write(new byte[5]);
            char[] buffer = new char[8192];
            byte[] encoded = new byte[buffer.length * 2];
            for (int n; (n = in.read(buffer)) > 0; ) {
                for (int i = 0; i < n; i++) {
                    encoded[2 * i] = (byte) buffer[i];
                    encoded[2 * i + 1] = (byte) (buffer[i] >> 8);
                }
                out.write(encoded, 0, 2 * n);
                chars += n;
            }
            long length = chars * 2;
            if (length > Integer.MAX_VALUE) {
                throw new IOException("A text body larger than 1 GiB of characters cannot be sent to Artemis.");
            }
            out.seek(0);
            out.writeByte(1);
            out.writeInt((int) length);
        }
    }

    /** Turns an Artemis text body stream into UTF-8, writing at most {@code limit} bytes. */
    static final class TextDecoder extends OutputStream {

        private final OutputStream out;
        private final CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder();
        private final byte[] header = new byte[5];
        private int headerRead;
        private int low = -1;
        private final CharBuffer chars = CharBuffer.allocate(4096);
        private final ByteBuffer bytes = ByteBuffer.allocate(4096 * 3 + 8);
        private long written;
        private final long limit;

        TextDecoder(OutputStream out, long limit) {
            this.out = out;
            this.limit = limit;
        }

        @Override
        public void write(int b) throws IOException {
            if (headerRead < header.length) {
                header[headerRead++] = (byte) b;
                return;
            }
            if (low < 0) {
                low = b & 0xFF;
                return;
            }
            chars.put((char) (low | (b & 0xFF) << 8));
            low = -1;
            if (!chars.hasRemaining()) {
                drain(false);
            }
        }

        private void drain(boolean end) throws IOException {
            chars.flip();
            encoder.encode(chars, bytes, end);
            chars.compact();
            bytes.flip();
            int n = (int) Math.min(bytes.remaining(), Math.max(0, limit - written));
            out.write(bytes.array(), bytes.position(), n);
            written += n;
            bytes.clear();
        }

        @Override
        public void close() throws IOException {
            drain(true);
            out.flush();
        }
    }

    /** Keeps at most {@code limit} bytes and remembers whether more came. */
    static final class CappedOutput extends ByteArrayOutputStream {

        private final long limit;
        private boolean cut;

        CappedOutput(long limit) {
            this.limit = limit;
        }

        @Override
        public synchronized void write(int b) {
            if (count < limit) {
                super.write(b);
            } else {
                cut = true;
            }
        }

        @Override
        public synchronized void write(byte[] b, int off, int len) {
            int room = (int) Math.max(0, Math.min(len, limit - count));
            super.write(b, off, room);
            if (room < len) {
                cut = true;
            }
        }

        boolean cut() {
            return cut;
        }
    }
}
