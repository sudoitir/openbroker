/**
 * The RabbitMQ driver: a management HTTP API client and an AMQP 0-9-1 transport.
 *
 * <p>Endpoints: the management API is reached at {@code MANAGEMENT} endpoints with protocol {@code http} or
 * {@code https}, as {@code host:port[/path-prefix]}, on cluster endpoints or per node; any node answers for the whole
 * cluster. Messages move at optional {@code DATA} endpoints with protocol {@code amqp} or {@code amqps}; without one,
 * browse, count and send fail {@code FAILED_PRECONDITION} and notifications are polled.
 *
 * <p>Credentials are asked for on each plane: a user name and password (HTTP Basic, AMQP PLAIN), a bearer token (HTTP
 * bearer, AMQP PLAIN with an empty user) or none (AMQP {@code EXTERNAL} with a client certificate).
 *
 * <p>Unavailable, and why: browse exists only on streams (a classic or quorum queue cannot be read without
 * consuming it), count by a filter and bulk message actions are not offered, and the stream protocol is not used.
 */
@NullMarked
package io.github.sudoitir.openbroker.rabbitmq;

import org.jspecify.annotations.NullMarked;
