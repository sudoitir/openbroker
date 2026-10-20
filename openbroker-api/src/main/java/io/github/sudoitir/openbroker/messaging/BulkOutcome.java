package io.github.sudoitir.openbroker.messaging;

import java.util.List;

/** What a bulk action did: how many it handled, and which selected messages it did not, and why. */
public record BulkOutcome(long done, List<MessageRef> notDone, String notDoneReason) {

    public BulkOutcome {
        notDone = List.copyOf(notDone);
    }

    public static BulkOutcome of(long done) {
        return new BulkOutcome(done, List.of(), "");
    }
}
