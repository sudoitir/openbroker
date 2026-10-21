package io.github.sudoitir.openbroker;

/** The standard error reasons, in the {@value #DOMAIN} domain. */
public final class ErrorReasons {

    public static final String DOMAIN = "openbroker.io";

    /** Metadata key: the plane, {@code MANAGEMENT} or {@code DATA}, whose account a failure is about. */
    public static final String PLANE = "openbroker.plane";

    /** Metadata key: the id of the resource a refusal or failure is about. */
    public static final String RESOURCE = "openbroker.resource";

    // Reaching the system.
    public static final String TLS_HANDSHAKE_FAILED = "TLS_HANDSHAKE_FAILED";
    public static final String CREDENTIALS_REJECTED = "CREDENTIALS_REJECTED";
    public static final String NOT_AUTHORIZED = "NOT_AUTHORIZED";
    public static final String UNREACHABLE = "UNREACHABLE";
    public static final String UNEXPECTED_SYSTEM = "UNEXPECTED_SYSTEM";
    public static final String ENDPOINT_NOT_FOUND = "ENDPOINT_NOT_FOUND";
    public static final String UNSUPPORTED_VERSION = "UNSUPPORTED_VERSION";
    public static final String RATE_LIMITED = "RATE_LIMITED";
    public static final String BAD_RESPONSE = "BAD_RESPONSE";
    public static final String RESPONSE_TOO_LARGE = "RESPONSE_TOO_LARGE";

    // Resources.
    public static final String RESOURCE_GONE = "RESOURCE_GONE";
    public static final String RESOURCE_EXISTS = "RESOURCE_EXISTS";
    public static final String HAS_CHILDREN = "HAS_CHILDREN";
    public static final String HAS_CONSUMERS = "HAS_CONSUMERS";
    public static final String MAX_CONSUMERS_REACHED = "MAX_CONSUMERS_REACHED";
    public static final String IMMUTABLE_FIELD = "IMMUTABLE_FIELD";
    public static final String INVALID_ARGUMENT = "INVALID_ARGUMENT";
    public static final String PRECONDITION_FAILED = "PRECONDITION_FAILED";
    /** A delete asked for an empty resource and it still holds messages. */
    public static final String NOT_EMPTY = "NOT_EMPTY";

    // Messages.
    public static final String DUPLICATE_REJECTED = "DUPLICATE_REJECTED";
    public static final String DESTINATION_FULL = "DESTINATION_FULL";
    public static final String INVALID_FILTER = "INVALID_FILTER";

    /** A sender's buffer had no room within its {@code maxBlock}. */
    public static final String SEND_BUFFER_FULL = "SEND_BUFFER_FULL";
    /** A message was settled on a link that has since recovered; it will be delivered again. */
    public static final String DELIVERY_STALE = "DELIVERY_STALE";
    /** A transaction was rolled back by a loss of its link. */
    public static final String TRANSACTION_ROLLED_BACK = "TRANSACTION_ROLLED_BACK";
    /** The system cannot do what a link asked for. */
    public static final String NOT_SUPPORTED = "NOT_SUPPORTED";

    // The caller's interceptor.
    public static final String CALL_REFUSED = "CALL_REFUSED";

    private ErrorReasons() {}
}
