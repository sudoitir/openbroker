package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.messaging.LinkState;
import io.github.sudoitir.openbroker.messaging.Processor;
import java.time.Duration;

/** A processor with the lanes' shared resources, released when it stops. */
final class CoreProcessor implements Processor {

    private final TransportEnv env;
    private final Processor engine;
    private final CoreLanes lanes;

    CoreProcessor(TransportEnv env, Processor engine, CoreLanes lanes) {
        this.env = env;
        this.engine = engine;
        this.lanes = lanes;
    }

    @Override
    public String id() {
        return engine.id();
    }

    @Override
    public LinkState state() {
        return engine.state();
    }

    @Override
    public void stop(Duration grace) {
        try {
            engine.stop(grace);
        } finally {
            lanes.close();
            env.forget(this);
        }
    }

    @Override
    public void close() {
        try {
            engine.close();
        } finally {
            lanes.close();
            env.forget(this);
        }
    }
}
