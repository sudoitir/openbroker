# ADR-0035: A send the system routed nowhere fails `NOT_ROUTED`

- **Status**: accepted
- **Date**: 2026-11-01
- **Deciders**: Mahdi Amirabdollahi

## Context

A publish to a RabbitMQ exchange with no matching binding is accepted and dropped, unless the publisher sets
`mandatory`, in which case the broker returns it (reply 312, `NO_ROUTE`) before confirming. Other systems behave
alike (an SNS or Service Bus topic with no subscription). The SDK had no reason for "the system took the message
and no subscription received it", so a driver could only call it a success, which loses the message silently,
or invent a reason of its own.

## Decision

`ErrorReasons.NOT_ROUTED`, with status `FAILED_PRECONDITION`. A driver reports it whenever the system tells it a
message was routed nowhere; the RabbitMQ driver always publishes with `mandatory` and confirms, so it always can.
A driver whose system drops unroutable messages without saying so does not guess.

## Consequences

- A sender learns at once that nothing will receive what it sent.
- Adding a reason is additive.

## Alternatives considered

- **Treat it as success**: silent loss.
- **`NOT_FOUND`**: the exchange exists; what is missing is a route, and a caller must be able to tell the two apart.
