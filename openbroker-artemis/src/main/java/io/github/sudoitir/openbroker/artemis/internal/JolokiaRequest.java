package io.github.sudoitir.openbroker.artemis.internal;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * One entry in a Jolokia request. A batch sends a JSON array of these; a single call sends one
 * object. Only the fields relevant to {@code type} are serialised.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record JolokiaRequest(
        String type,
        String mbean,
        @Nullable List<String> attribute,
        @Nullable String operation,
        @Nullable List<Object> arguments) {

    /** Read one or more attributes of an MBean. In a POST body attributes are an array. */
    public static JolokiaRequest read(String mbean, String... attributes) {
        return new JolokiaRequest("read", mbean, List.of(attributes), null, null);
    }

    /** Read every attribute of an MBean: Jolokia treats an omitted {@code attribute} as all. */
    public static JolokiaRequest readAll(String mbean) {
        return new JolokiaRequest("read", mbean, null, null, null);
    }

    /** Invoke an operation; {@code operation} includes its signature. */
    public static JolokiaRequest exec(String mbean, String operation, Object... arguments) {
        return new JolokiaRequest("exec", mbean, null, operation, List.of(arguments));
    }

    public static JolokiaRequest search(String pattern) {
        return new JolokiaRequest("search", pattern, null, null, null);
    }

    /** Leaves the arguments out: an exec can carry a password, and this may reach a log. */
    @Override
    public String toString() {
        return "JolokiaRequest[type=" + type + ", mbean=" + mbean + ", attribute=" + attribute + ", operation="
                + operation + ", arguments=" + (arguments == null ? "none" : arguments.size() + " withheld") + "]";
    }
}
