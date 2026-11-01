package io.github.sudoitir.openbroker.rabbitmq.admin;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.Dispatch;
import io.github.sudoitir.openbroker.ErrorInfo;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;

/**
 * The whole mapping of RabbitMQ failures (HTTP status, AMQP reply code, IO, TLS) to {@link BrokerException}, and the
 * only place one is built.
 */
public final class RabbitMqErrors {

    private RabbitMqErrors() {}

    /** The failure of a part of the driver that a later change fills in. */
    public static BrokerException unimplemented(String what) {
        return new BrokerException(
                Status.UNIMPLEMENTED,
                ErrorInfo.of(ErrorReasons.NOT_SUPPORTED),
                Dispatch.NOT_SENT,
                null,
                "Not implemented yet: " + what + ".");
    }

    /** The failure for a management API response with {@code httpStatus}, to {@code operation}. */
    public static BrokerException fromStatus(int httpStatus, String operation) {
        throw unimplemented("RabbitMqErrors.fromStatus");
    }

    /** The failure for an AMQP channel or connection close with {@code replyCode}. */
    public static BrokerException fromReply(int replyCode, String replyText) {
        throw unimplemented("RabbitMqErrors.fromReply");
    }

    /** The failure for an I/O or TLS error; {@code sent} says whether the request may have reached the system. */
    public static BrokerException fromIo(Throwable cause, boolean sent) {
        throw unimplemented("RabbitMqErrors.fromIo");
    }
}
