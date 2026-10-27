package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import io.github.sudoitir.openbroker.messaging.MessageHandler;
import io.github.sudoitir.openbroker.messaging.Processor;
import io.github.sudoitir.openbroker.messaging.ProcessorOptions;
import io.github.sudoitir.openbroker.messaging.Transport;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.Scope;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Owns the processors an application registers, so that they follow the application's lifecycle.
 *
 * <p>{@code Transport.process} starts a processor at once, and a message could reach a handler before the
 * rest of the context is ready. A processor registered here starts only when the context is ready,
 * and on shutdown every processor stops within its {@code stopGrace} before any connection closes.
 *
 * <p>A transport the application opened itself can be registered too; the starter never closes it.
 */
public final class OpenBrokerProcessors implements SmartLifecycle {

    /** After the web server starts and before it stops: a handler runs only while the application can serve. */
    static final int PHASE = SmartLifecycle.DEFAULT_PHASE - 2048;

    private record Registration(
            Transport transport,
            ResourceId subscription,
            Scope scope,
            ProcessorOptions options,
            MessageHandler handler) {}

    private final BeanFactory beans;
    private final OpenBrokerConnections connections;
    private final List<Registration> pending = new ArrayList<>();
    private final List<Started> running = new ArrayList<>();
    private boolean started;
    private boolean stopped;

    private record Started(Processor processor, ProcessorOptions options) {}

    OpenBrokerProcessors(BeanFactory beans, OpenBrokerConnections connections) {
        this.beans = beans;
        this.connections = connections;
    }

    /**
     * Registers a processor on a configured connection, with that connection's processor options.
     *
     * @param connection the connection's name
     * @param subscription what to process
     * @param scope where
     * @param handler handles each message
     */
    public void register(String connection, ResourceId subscription, Scope scope, MessageHandler handler) {
        register(
                connection,
                subscription,
                scope,
                connections.connection(connection).processor(),
                handler);
    }

    /** {@link #register(String, ResourceId, Scope, MessageHandler)} with options of its own. */
    public void register(
            String connection, ResourceId subscription, Scope scope, ProcessorOptions options, MessageHandler handler) {
        register(
                beans.getBean(connection + OpenBrokerConnectionRegistrar.TRANSPORT, Transport.class),
                subscription,
                scope,
                options,
                handler);
    }

    /** Registers a processor on any transport, such as one of a connection the application opened itself. */
    public synchronized void register(
            Transport transport,
            ResourceId subscription,
            Scope scope,
            ProcessorOptions options,
            MessageHandler handler) {
        if (stopped) {
            throw new IllegalStateException("The application is shutting down; no processor can be registered");
        }
        Registration registration = new Registration(transport, subscription, scope, options, handler);
        if (started) {
            run(registration);
        } else {
            pending.add(registration);
        }
    }

    @Override
    public synchronized void start() {
        started = true;
        pending.forEach(this::run);
        pending.clear();
    }

    private void run(Registration registration) {
        running.add(new Started(
                registration.transport.process(
                        registration.subscription, registration.scope, registration.options, registration.handler),
                registration.options));
    }

    /** Stops every processor within its grace time, all at once, so that shutdown takes the longest grace and not the sum. */
    @Override
    public void stop() {
        List<Started> stopping;
        synchronized (this) {
            stopped = true;
            stopping = new ArrayList<>(running);
            running.clear();
            pending.clear();
        }
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (Started started : stopping.reversed()) {
                executor.execute(() -> started.processor.stop(started.options.stopGrace()));
            }
        }
    }

    @Override
    public synchronized boolean isRunning() {
        return started && !stopped;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }
}
