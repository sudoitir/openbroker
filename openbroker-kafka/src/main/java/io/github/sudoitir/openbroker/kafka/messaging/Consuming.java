package io.github.sudoitir.openbroker.kafka.messaging;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.messaging.LinkState;
import java.time.Duration;
import java.util.Collection;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;

/**
 * What the lanes of a processor ask of the member of a group that feeds them: a consumer-group
 * member ({@link Consumption}) or a share-group member ({@link ShareConsumption}). Every call is
 * safe from a handler thread.
 */
interface Consuming extends AutoCloseable {

    /** Who runs the handlers. Both calls are made on the poll thread and must not block. */
    interface Sink {

        /** A record ready to be handled. */
        void deliver(Delivery delivery);

        /** Partitions are going: drop the deliveries queued for them that no handler has taken. */
        void retract(Collection<TopicPartition> partitions);
    }

    /** Starts polling. */
    void start();

    LinkState state();

    /** The failure the last poll ended with (a denied topic, a rejected login), or null while polling works. */
    @Nullable BrokerException failure();

    /** The record is done: it is not delivered again. */
    void settle(Delivery delivery);

    /** The record is given up: it is not delivered again, and the system dead-letters it where it can. */
    void reject(Delivery delivery);

    /**
     * Hands the record back: it is delivered again after {@code delay}, with its attempt raised when
     * {@code failed}.
     */
    void requeue(Delivery delivery, Duration delay, boolean failed);

    /** Stops polling, settles what was settled and leaves the group. */
    @Override
    void close();
}
