package io.github.sudoitir.openbroker.rabbitmq.messaging;

import io.github.sudoitir.openbroker.messaging.BrowsePage;
import io.github.sudoitir.openbroker.messaging.BrowseRequest;
import io.github.sudoitir.openbroker.messaging.Disposition;
import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Message;
import io.github.sudoitir.openbroker.messaging.Receiver;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqCluster;
import io.github.sudoitir.openbroker.rabbitmq.admin.RabbitMqErrors;
import java.time.Duration;
import java.util.List;

/**
 * The browsing {@link Receiver} of a stream and the engine behind {@code Messages.browse}: starts at an offset, bounded
 * by production time, and ends at the committed offset at open. It acknowledges what it reads, which never removes
 * anything from a stream.
 */
final class StreamReader implements Receiver {

    StreamReader(RabbitMqCluster cluster, ResourceId stream) {}

    /** Reads one page of the stream as {@code request} asks. */
    BrowsePage browse(BrowseRequest request) {
        throw RabbitMqErrors.unimplemented("StreamReader.browse");
    }

    @Override
    public String id() {
        throw RabbitMqErrors.unimplemented("StreamReader.id");
    }

    @Override
    public LinkState state() {
        throw RabbitMqErrors.unimplemented("StreamReader.state");
    }

    @Override
    public List<Message> receive(int max, Duration timeout) {
        throw RabbitMqErrors.unimplemented("StreamReader.receive");
    }

    @Override
    public void settle(Message message, Disposition disposition) {
        throw RabbitMqErrors.unimplemented("StreamReader.settle");
    }

    @Override
    public Sender sender(SendTarget target) {
        throw RabbitMqErrors.unimplemented("StreamReader.sender");
    }

    @Override
    public void commit() {
        throw RabbitMqErrors.unimplemented("StreamReader.commit");
    }

    @Override
    public void rollback() {
        throw RabbitMqErrors.unimplemented("StreamReader.rollback");
    }

    @Override
    public void close() {}
}
