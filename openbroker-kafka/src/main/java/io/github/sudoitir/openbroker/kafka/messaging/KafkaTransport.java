package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.kafka.admin.KafkaCluster;
import io.github.sudoitir.openbroker.kafka.admin.KafkaErrors;
import io.github.sudoitir.openbroker.messaging.LinkListener;
import io.github.sudoitir.openbroker.messaging.MessageHandler;
import io.github.sudoitir.openbroker.messaging.Processor;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.Receiver;
import io.github.sudoitir.openbroker.messaging.ReceiverOptions;
import io.github.sudoitir.openbroker.messaging.SendTarget;
import io.github.sudoitir.openbroker.messaging.Sender;
import io.github.sudoitir.openbroker.messaging.SenderOptions;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.messaging.TransportCapabilities;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;

/**
 * The messaging surface of a Kafka connection: the one place the connection reaches the senders,
 * receivers and processors. Its client settings come from {@link KafkaCluster#clientConfig}.
 */
public final class KafkaTransport implements Transport, AutoCloseable {

    private final KafkaCluster cluster;

    public KafkaTransport(KafkaCluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public TransportCapabilities capabilities() {
        throw notYet();
    }

    @Override
    public Sender sender(SendTarget target, Scope scope, SenderOptions options) {
        throw notYet();
    }

    @Override
    public Receiver receiver(ResourceId subscription, Scope scope, ReceiverOptions options) {
        throw notYet();
    }

    @Override
    public Processor process(ResourceId subscription, Scope scope, ProcessorOptions options, MessageHandler handler) {
        throw notYet();
    }

    @Override
    public AutoCloseable watch(LinkListener listener) {
        throw notYet();
    }

    @Override
    public void close() {}

    private static RuntimeException notYet() {
        return KafkaErrors.unimplemented("send or receive messages yet");
    }
}
