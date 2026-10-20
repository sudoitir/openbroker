package io.github.sudoitir.openbroker.artemis.internal;

import io.github.sudoitir.openbroker.model.ConditionStatus;
import io.github.sudoitir.openbroker.model.Designation;
import io.github.sudoitir.openbroker.model.Serving;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;

/**
 * Artemis' HA attributes in the SDK's terms. A configured backup is {@code BACKUP} whatever it is
 * doing now; a clustered server is {@code PRIMARY}; anything else is not in an HA group.
 */
public final class Ha {

    /** The attributes one read of the broker MBean asks for. */
    public static final String[] ATTRIBUTES = {
        "Active", "Started", "Backup", "ReplicaSync", "NodeID", "Clustered", "Version"
    };

    private Ha() {}

    public static Designation designation(@Nullable Boolean backup, @Nullable Boolean clustered) {
        if (Boolean.TRUE.equals(backup)) {
            return Designation.BACKUP;
        }
        if (Boolean.TRUE.equals(clustered)) {
            return Designation.PRIMARY;
        }
        return Designation.NONE;
    }

    public static Serving serving(@Nullable Boolean active) {
        if (active == null) {
            return Serving.UNKNOWN;
        }
        return active ? Serving.ACTIVE : Serving.STANDBY;
    }

    public static ConditionStatus started(@Nullable Boolean started) {
        if (started == null) {
            return ConditionStatus.UNKNOWN;
        }
        return started ? ConditionStatus.TRUE : ConditionStatus.FALSE;
    }

    public static @Nullable Boolean bool(@Nullable JsonNode ha, String name) {
        JsonNode v = ha == null ? null : ha.get(name);
        if (v == null || v.isNull()) {
            return null;
        }
        return v.isBoolean() ? v.asBoolean() : Boolean.parseBoolean(v.asString());
    }

    public static @Nullable String text(@Nullable JsonNode ha, String name) {
        JsonNode v = ha == null ? null : ha.get(name);
        return v == null || v.isNull() ? null : v.asString();
    }
}
