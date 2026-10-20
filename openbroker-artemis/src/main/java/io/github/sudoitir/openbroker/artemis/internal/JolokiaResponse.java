package io.github.sudoitir.openbroker.artemis.internal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * One entry of a Jolokia response.
 *
 * <ul>
 *   <li>In a batch the HTTP status is 200 even when entries failed: check {@link #ok()} per entry.
 *   <li>The {@code value} of {@code listNetworkTopology()} and the {@code listX} operations is a
 *       JSON-encoded string: use {@link #valueParsed(ObjectMapper)}.
 * </ul>
 *
 * @param timestamp the broker's wall clock when it answered, in epoch seconds; the only reading of
 *     its clock that costs no extra call. A proxy may strip it.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record JolokiaResponse(
        int status,
        @Nullable JsonNode value,
        @Nullable String error,
        @JsonProperty("error_type") @Nullable String errorType,
        @Nullable Long timestamp) {

    public boolean ok() {
        return status == 200 && error == null;
    }

    /** Why this entry failed, for a message. */
    public String failure() {
        return error != null ? error : "status " + status;
    }

    /**
     * One attribute's value. A read whose attribute is a list (which this driver always sends) comes
     * back as a map keyed by attribute name; a scalar read comes back as the value itself.
     */
    public @Nullable JsonNode attribute(String name) {
        if (value == null) {
            return null;
        }
        return value.isObject() && value.has(name) ? value.get(name) : value;
    }

    /** The value, parsed again when it is a JSON string. */
    public @Nullable JsonNode valueParsed(ObjectMapper mapper) {
        if (value != null && value.isString()) {
            try {
                return mapper.readTree(value.asString());
            } catch (RuntimeException e) {
                throw new IllegalStateException("Jolokia value was a string but not JSON", e);
            }
        }
        return value;
    }
}
