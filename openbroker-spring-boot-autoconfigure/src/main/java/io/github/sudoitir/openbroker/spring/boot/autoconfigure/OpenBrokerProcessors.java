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
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.ContextClosedEvent;

/**
 * Owns the processors an application registers, so that they follow the application's lifecycle.
 *
 * <p>{@code Transport.process} starts a processor at once, and a message could reach a handler before the
 * rest of the context is ready. A processor registered here starts only when the context is ready,
 * and on shutdown every processor stops within its {@code stopGrace} before any connection closes.
 *
 * <p>A transport the application opened itself can be registered too; the starter never closes it.
 */
public final class OpenBrokerProcessors implements SmartLifecycle, ApplicationListener<ApplicationEvent> {

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
    private final List<Registration> registrations = new ArrayList<>();
    private final List<Started> running = new ArrayList<>();
    private boolean ready;
    private boolean started;
    private boolean closed;

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
        String beanName = connection + OpenBrokerConnectionRegistrar.TRANSPORT;
        Object bean = beans.containsBean(beanName) ? beans.getBean(beanName) : null;
        if (!(bean instanceof Transport transport)) {
            throw new IllegalArgumentException("Connection '" + connection + "' has no messaging transport");
        }
        register(transport, subscription, scope, options, handler);
    }

    /**
     * Registers a processor on any transport, such as one of a connection the application opened itself.
     * It starts when the context is ready, or at once if it already is; a context that is stopped and
     * started again starts it again.
     *
     * @throws IllegalStateException once the context is closing
     */
    public synchronized void register(
            Transport transport,
            ResourceId subscription,
            Scope scope,
            ProcessorOptions options,
            MessageHandler handler) {
        if (closed) {
            throw new IllegalStateException("The application is shutting down; no processor can be registered");
        }
        Registration registration = new Registration(transport, subscription, scope, options, handler);
        registrations.add(registration);
        if (started) {
            run(registration);
        }
    }

    /**
     * Starts the processors when the application is ready, which is after its runners have run, and again
     * when a context that was stopped is started. The context's own start, in the middle of its refresh,
     * does nothing: a handler must not run before the application's runners have finished.
     */
    @Override
    public synchronized void start() {
        if (ready && !started) {
            started = true;
            registrations.forEach(this::run);
        }
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
            started = false;
            stopping = new ArrayList<>(running);
            running.clear();
        }
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (Started processor : stopping.reversed()) {
                executor.execute(() -> processor.processor.stop(processor.options.stopGrace()));
            }
        }
    }

    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        if (event instanceof ApplicationReadyEvent) {
            synchronized (this) {
                ready = true;
            }
            start();
        } else if (event instanceof ContextClosedEvent) {
            synchronized (this) {
                closed = true;
            }
        }
    }

    @Override
    public synchronized boolean isRunning() {
        return started;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }
}
