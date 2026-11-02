package io.github.sudoitir.openbroker.rabbitmq.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.messaging.Cursor;
import io.github.sudoitir.openbroker.messaging.Position;
import io.github.sudoitir.openbroker.model.PartitionId;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Shard;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StreamOffsetsTest {

    @Test
    void positionsMapToTheOffsetArgument() {
        Instant time = Instant.parse("2026-01-02T03:04:05Z");

        assertThat(StreamOffsets.argument(Position.earliest())).isEqualTo("first");
        assertThat(StreamOffsets.argument(Position.latest())).isEqualTo("next");
        assertThat(StreamOffsets.argument(new Position.AtOffsets(Map.of(StreamOffsets.PARTITION, 42L))))
                .isEqualTo(42L);
        assertThat(StreamOffsets.argument(new Position.AtTime(time))).isEqualTo(Date.from(time));
        assertThat(StreamOffsets.LAST).isEqualTo("last");
    }

    @Test
    void aCursorStartsAfterItsOffset() {
        Cursor cursor = StreamOffsets.cursor(9, Instant.ofEpochMilli(1234));

        assertThat(StreamOffsets.argument(Position.after(cursor))).isEqualTo(10L);
        assertThat(StreamOffsets.cursorOffset(cursor)).isEqualTo(9);
        assertThat(StreamOffsets.cursorFirstTimestamp(cursor)).isEqualTo(Instant.ofEpochMilli(1234));
    }

    @Test
    void positionsAStreamCannotStartAtAreRefused() {
        Position other = new Position.AtOffsets(Map.of(Shard.partition(PartitionId.of("1")), 1L));
        Position negative = new Position.AtOffsets(Map.of(StreamOffsets.PARTITION, -1L));
        Position two =
                new Position.AtOffsets(Map.of(StreamOffsets.PARTITION, 1L, Shard.partition(PartitionId.of("1")), 2L));
        Position snapshot = new Position.AtSnapshot(ResourceId.of("rabbitmq.stream", "/", "s"));
        Position junk = Position.after(new Cursor("***"));
        Position noNumbers = Position.after(new Cursor("YWJj"));

        for (Position position : new Position[] {other, negative, two, snapshot, junk, noNumbers}) {
            assertThatThrownBy(() -> StreamOffsets.argument(position))
                    .isInstanceOfSatisfying(
                            BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT));
        }
    }

    @Test
    void theDeliveredHeaderIsTheOffset() {
        assertThat(StreamOffsets.offset(Map.of("x-stream-offset", 7L))).isEqualTo(7);
        assertThat(StreamOffsets.offset(Map.of("x-stream-offset", 8))).isEqualTo(8);
        Map<String, Object> noHeaders = Map.of();
        assertThatThrownBy(() -> StreamOffsets.offset(noHeaders))
                .isInstanceOfSatisfying(
                        BrokerException.class, e -> assertThat(e.status()).isEqualTo(Status.INVALID_ARGUMENT));
    }
}
