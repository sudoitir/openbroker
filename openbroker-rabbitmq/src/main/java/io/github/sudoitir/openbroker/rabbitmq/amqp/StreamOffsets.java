package io.github.sudoitir.openbroker.rabbitmq.amqp;

import io.github.sudoitir.openbroker.messaging.Position;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;
import java.util.Map;

/** Maps a {@link Position} to the {@code x-stream-offset} argument, and the {@code x-stream-offset} header to a position. */
public final class StreamOffsets {

    private StreamOffsets() {}

    public static Object argument(Position position) {
        throw RabbitMqErrors.unimplemented("StreamOffsets.argument");
    }

    public static long offset(Map<String, Object> headers) {
        throw RabbitMqErrors.unimplemented("StreamOffsets.offset");
    }
}
