package io.github.sudoitir.openbroker.rabbitmq.amqp;

import io.github.sudoitir.openbroker.messaging.Cursor;
import io.github.sudoitir.openbroker.messaging.Position;
import io.github.sudoitir.openbroker.model.PartitionId;
import io.github.sudoitir.openbroker.model.Shard;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.Map;

/**
 * Maps a {@link Position} to the {@code x-stream-offset} argument, and the {@code x-stream-offset} header to a position.
 *
 * <p>A stream is read as one partition named {@code 0}. A time position is an AMQP timestamp, which has second
 * granularity, and the stream starts at the first chunk that holds it, so a read may begin a little before the time.
 */
public final class StreamOffsets {

    /** The name of the argument and of the delivered header. */
    public static final String HEADER = "x-stream-offset";

    /** The first offset the stream still retains. */
    public static final String FIRST = "first";

    /** The last chunk written. */
    public static final String LAST = "last";

    /** The offset of the next message to be written. */
    public static final String NEXT = "next";

    /** The partition every stream message is read from. */
    public static final Shard PARTITION = Shard.partition(PartitionId.of("0"));

    private static final char CURSOR_SEPARATOR = '|';

    private StreamOffsets() {}

    /**
     * The {@code x-stream-offset} argument that starts a consumer at {@code position}: {@code "first"},
     * {@code "next"}, a {@code Long} offset or a {@link Date} timestamp.
     *
     * @throws io.github.sudoitir.openbroker.BrokerException {@code INVALID_ARGUMENT} for a position a stream cannot
     *     start at, or a malformed cursor
     */
    public static Object argument(Position position) {
        return switch (position) {
            case Position.Earliest _ -> FIRST;
            case Position.Latest _ -> NEXT;
            case Position.AtOffsets at -> atOffset(at);
            case Position.AtTime(Instant time) -> Date.from(time);
            case Position.After(Cursor cursor) -> cursorOffset(cursor) + 1L;
            case Position.AtSnapshot _ ->
                throw RabbitMqErrors.invalid("A RabbitMQ stream has no snapshots to read from.");
        };
    }

    /**
     * The offset of a delivered message, from its headers.
     *
     * @throws io.github.sudoitir.openbroker.BrokerException {@code INVALID_ARGUMENT} when the header is missing
     */
    public static long offset(Map<String, Object> headers) {
        if (headers.get(HEADER) instanceof Number number) {
            return number.longValue();
        }
        throw RabbitMqErrors.invalid("The delivery carries no " + HEADER + " header: it is not from a stream.");
    }

    /** A cursor that names the message at {@code offset} of a stream whose first message was written at {@code firstTimestamp}. */
    public static Cursor cursor(long offset, Instant firstTimestamp) {
        String text = offset + String.valueOf(CURSOR_SEPARATOR) + firstTimestamp.toEpochMilli();
        return new Cursor(
                Base64.getUrlEncoder().withoutPadding().encodeToString(text.getBytes(StandardCharsets.UTF_8)));
    }

    /** The offset a cursor names. */
    public static long cursorOffset(Cursor cursor) {
        return parse(cursor)[0];
    }

    /** The time the first message of the stream was written, as the cursor remembers it. */
    public static Instant cursorFirstTimestamp(Cursor cursor) {
        return Instant.ofEpochMilli(parse(cursor)[1]);
    }

    private static Object atOffset(Position.AtOffsets at) {
        if (at.offsets().size() != 1 || !at.offsets().containsKey(PARTITION)) {
            throw RabbitMqErrors.invalid(
                    "A RabbitMQ stream is read from the partition '0' alone: give one offset for it.");
        }
        long offset = at.offsets().get(PARTITION);
        if (offset < 0) {
            throw RabbitMqErrors.invalid("A stream offset is zero or more, not " + offset + ".");
        }
        return offset;
    }

    private static long[] parse(Cursor cursor) {
        try {
            String text = new String(Base64.getUrlDecoder().decode(cursor.token()), StandardCharsets.UTF_8);
            int split = text.indexOf(CURSOR_SEPARATOR);
            if (split > 0) {
                return new long[] {Long.parseLong(text.substring(0, split)), Long.parseLong(text.substring(split + 1))};
            }
        } catch (IllegalArgumentException _) {
            // Not base64 or not numbers: reported below.
        }
        throw RabbitMqErrors.invalid("The cursor is not one this driver issued for a stream.");
    }
}
