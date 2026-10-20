package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.Status;

/** The failure for what Artemis cannot do. */
public final class Unimplemented {

    private Unimplemented() {}

    public static BrokerException of(String what) {
        return new BrokerException(
                Status.UNIMPLEMENTED,
                ErrorInfo.of("UNIMPLEMENTED"),
                Dispatch.NOT_SENT,
                null,
                "Artemis cannot " + what + ".");
    }
}
