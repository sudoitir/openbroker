package io.github.sudoitir.openbroker.messaging;

import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Shard;
import java.util.Objects;

/**
 * Where one message is: the subscription or partition it was read from, the shard, and an opaque
 * position the driver understands (an Artemis message id, a Kafka offset).
 */
public record MessageRef(ResourceId source, Shard shard, String position) {

    public MessageRef {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(shard, "shard");
        Objects.requireNonNull(position, "position");
    }
}
