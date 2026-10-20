package io.github.sudoitir.openbroker;

/**
 * The caller's control over every management and control call a driver makes: rate limiting,
 * health tracking, metrics, tracing, or refusing calls in a context where none should happen.
 *
 * <p>A driver asks for a permit before each request it sends, including each request inside a
 * single facet call, and reports how the request ended on that permit. Per-message sends and
 * receives inside a {@code Receiver} or {@code Sender} are not intercepted. {@link #admit} may block
 * to shape load; it refuses a call by throwing a {@link BrokerException} with reason {@link
 * ErrorReasons#CALL_REFUSED}. It may run on a driver thread; drivers carry the caller's context
 * there through {@link ConnectOptions#contextPropagation()}.
 */
@FunctionalInterface
public interface CallInterceptor {

    Permit admit(CallInfo call);

    /** Admits every call at once and records nothing. */
    static CallInterceptor none() {
        return call -> Permit.unlimited();
    }
}
