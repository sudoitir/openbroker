package io.github.sudoitir.openbroker.messaging;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;

/**
 * Several observers as one. Each sees every operation in the order given; scopes close in reverse.
 * An observer that throws is logged and skipped for that operation, so it never stops another
 * observer or the operation itself.
 */
final class CompositeObserver implements MessagingObserver {

    private static final Logger LOG = System.getLogger(CompositeObserver.class.getName());

    private final List<MessagingObserver> observers;

    CompositeObserver(List<MessagingObserver> observers) {
        this.observers = List.copyOf(observers);
    }

    @Override
    public Observation onSend(OperationInfo op, OutboundMessage message, BiConsumer<String, String> carrier) {
        return each(o -> o.onSend(op, message, carrier));
    }

    @Override
    public Observation onReceive(OperationInfo op) {
        return each(o -> o.onReceive(op));
    }

    @Override
    public Observation onProcess(OperationInfo op, Message message) {
        return each(o -> o.onProcess(op, message));
    }

    @Override
    public Observation onSettle(OperationInfo op, Message message, Disposition disposition) {
        return each(o -> o.onSettle(op, message, disposition));
    }

    private Observation each(Function<MessagingObserver, Observation> start) {
        List<Observation> started = new ArrayList<>(observers.size());
        for (MessagingObserver observer : observers) {
            try {
                started.add(start.apply(observer));
            } catch (RuntimeException e) {
                failed(observer, e);
            }
        }
        return new Many(started);
    }

    private static void failed(Object observer, RuntimeException e) {
        LOG.log(Level.DEBUG, () -> "Observer " + observer.getClass().getName() + " failed; skipped", e);
    }

    private record Many(List<Observation> observations) implements Observation {

        @Override
        public AutoCloseable scope() {
            List<AutoCloseable> opened = new ArrayList<>(observations.size());
            for (Observation o : observations) {
                try {
                    opened.add(o.scope());
                } catch (RuntimeException e) {
                    failed(o, e);
                }
            }
            return () -> {
                for (int i = opened.size() - 1; i >= 0; i--) {
                    try {
                        opened.get(i).close();
                    } catch (Exception e) {
                        failed(opened.get(i), e instanceof RuntimeException r ? r : new IllegalStateException(e));
                    }
                }
            };
        }

        @Override
        public void received(List<Message> messages) {
            for (Observation o : observations) {
                try {
                    o.received(messages);
                } catch (RuntimeException e) {
                    failed(o, e);
                }
            }
        }

        @Override
        public void end(@Nullable Throwable error) {
            for (Observation o : observations) {
                try {
                    o.end(error);
                } catch (RuntimeException e) {
                    failed(o, e);
                }
            }
        }
    }
}
