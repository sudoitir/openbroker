# ADR-0033: The RabbitMQ mapping: management over HTTP, messages over AMQP 0-9-1, browse only on streams

- **Status**: accepted
- **Date**: 2026-11-01
- **Deciders**: Mahdi Amirabdollahi

## Context

RabbitMQ routes by exchange and binding inside virtual hosts, and has three queue types with different
semantics: classic and quorum queues are consumed destructively, and a stream is an append-only log read from an
offset. It is managed through the management plugin's HTTP API and moves messages over AMQP 0-9-1, and offers a
separate stream protocol on its own port and plugin. Its management API exposes no incarnation id for a queue,
exchange or policy.

The SDK promises that browse never consumes. On a classic or quorum queue the only way to read a message without
a consumer is `basic.get` followed by a requeue, which changes delivery counts, redelivery flags and order, and
on a quorum queue increments the delivery limit that dead-letters a poisoned message.

## Decision

- **Two channels.** The driver manages through the management HTTP API (JDK `HttpClient`, bounded responses, no
  redirects) and moves messages over AMQP 0-9-1 with `com.rabbitmq:amqp-client`. It does not use the stream
  protocol: AMQP 0-9-1 reads a stream from an offset with `x-stream-offset`, without a second port, plugin or
  native compression libraries.
- **Mapping.** A virtual host is a namespace; an exchange a destination; a classic queue, a quorum queue and a
  stream are three subscription kinds, because their verbs differ (a stream has no purge and no consuming
  receive); bindings, exchange-to-exchange bindings, shovels and federation links are routes; policies are
  policies and permissions access rules; connections, channels and consumers are clients. Quorum members are
  leader and follower placements, each marked in sync or not.
- **Browse only on streams.** Classic and quorum kinds do not declare `BROWSE`; asking is refused with
  `NOT_SUPPORTED` and a reason, before any broker call. Get-and-requeue is never offered as browse.
- **Settlement by acknowledgement.** Receives use manual acknowledgements and `basic.qos`; sends use publisher
  confirms and `mandatory`. There is no transacted batch.
- **Recovery is the SDK's.** The client's automatic connection and topology recovery are off; the driver's link
  supervisor reopens links, as for every driver.
- **Events** come from the event exchange where its plugin is enabled and the account may read it; otherwise a
  polling watch of consumers, and the capability says which.
- **Floor.** RabbitMQ 3.13 and later, read from `rabbitmq_version` in `/api/overview`.

## Consequences

- An operator can never lose or reorder a message by looking at it.
- Shovels, federation upstreams and users are read and delete only until the SDK accepts write-only secrets.
- A stream's message count is sampled by the server, so it is an estimate and says so.
- Etags cannot name an incarnation (ADR-0034).

## Alternatives considered

- **Get and requeue as browse.** Rejected: it is destructive in every way that matters to an operator.
- **The stream protocol client.** Rejected for now: a second port, plugin and dependency tree for what AMQP
  0-9-1 already does.
- **One queue kind with a type attribute.** Rejected: verbs are per kind, and a stream would claim a purge it
  does not have.
