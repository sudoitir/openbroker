package io.github.sudoitir.openbroker.messaging;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.Optional;
import org.jspecify.annotations.Nullable;

/**
 * A message body. A small body is held in memory; a large one streams from where the driver spooled
 * it, so a relay never needs it all in memory. A tombstone (a Kafka record with a null value) is
 * not an empty body.
 */
public final class MessageBody {

    /** Opens a fresh stream over a body. */
    @FunctionalInterface
    public interface Source {
        InputStream open() throws IOException;
    }

    private static final MessageBody EMPTY = new MessageBody(Blob.empty(), null, 0, false);
    private static final MessageBody TOMBSTONE = new MessageBody(null, null, 0, true);

    private final @Nullable Blob bytes;
    private final @Nullable Source source;
    private final long size;
    private final boolean tombstone;

    private MessageBody(@Nullable Blob bytes, @Nullable Source source, long size, boolean tombstone) {
        this.bytes = bytes;
        this.source = source;
        this.size = size;
        this.tombstone = tombstone;
    }

    public static MessageBody of(Blob bytes) {
        return bytes.size() == 0 ? EMPTY : new MessageBody(bytes, null, bytes.size(), false);
    }

    public static MessageBody of(byte[] bytes) {
        return of(Blob.of(bytes));
    }

    public static MessageBody empty() {
        return EMPTY;
    }

    public static MessageBody tombstone() {
        return TOMBSTONE;
    }

    /** A body read from {@code source} each time it is opened; {@code size} is its length in bytes. */
    public static MessageBody streamed(long size, Source source) {
        return new MessageBody(null, Objects.requireNonNull(source, "source"), size, false);
    }

    public long size() {
        return size;
    }

    public boolean isTombstone() {
        return tombstone;
    }

    /** The bytes, when they are held in memory. */
    public Optional<Blob> bytes() {
        return Optional.ofNullable(bytes);
    }

    /** A new stream over the whole body. */
    public InputStream openStream() {
        if (bytes != null) {
            return new ByteArrayInputStream(bytes.toByteArray());
        }
        if (source == null) {
            return InputStream.nullInputStream();
        }
        try {
            return source.open();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString() {
        return tombstone ? "MessageBody[tombstone]" : "MessageBody[" + size + " bytes]";
    }
}
