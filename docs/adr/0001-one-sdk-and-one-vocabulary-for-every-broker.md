# ADR-0001: One SDK and one vocabulary for every broker and queue service

- **Status**: accepted
- **Date**: 2026-10-20
- **Deciders**: Mahdi Amirabdollahi

## Context

A backend that works with message brokers (a bank's integration layer, an ESB, an operations tool)
talks to each one through its own client: the Artemis management API over Jolokia and its Core client,
Kafka's Admin and consumer clients, RabbitMQ's HTTP API and AMQP 0-9-1, a cloud service's REST API. Each
names the same ideas differently (address, topic, exchange; queue, subscription, consumer group), fails
differently and reports load differently. JMS and AMQP 1.0 standardise sending and receiving only;
management, metrics, HA state and topology have no standard.

Prior art shaped the design: JDBC and R2DBC (a small API, drivers found by `ServiceLoader`, a
`ConnectionFactories`-style entry point), CloudEvents (an event envelope, namespaced extensions),
Google's API Improvement Proposals (resource names, AIP-132 ordering, AIP-154 etags, AIP-193 errors),
OpenTelemetry's semantic conventions (dotted metric keys, UCUM units), and Kubernetes (preconditions
as part of the request, conditions on resources).

## Decision

OpenBroker is one framework-free Java SDK with one vocabulary for every broker and queue service. Every
public name must read right for Kafka, RabbitMQ, ActiveMQ Classic and Artemis, Pulsar, NATS JetStream,
Solace, SQS, Azure Service Bus and Google Pub/Sub. Behaviour only one system has is reached through
`ClusterConnection.unwrap` and a driver's own extension type; data only one system has is carried
under a namespaced attribute name (`artemis.routing_type`). Drivers implement an SPI and prove
themselves against a TCK on a real broker.

The SDK manages and observes brokers and moves messages for tools. A production messaging client
(delivery guarantees, flow control, failover contracts, an async API) is a separate change that builds
on the same vocabulary.

## Consequences

- A tool written against the API works with every driver for the capabilities that driver declares.
- Each name costs a check against ten systems; a name shaped by one system or by JMS is a defect.
- Some native detail is one `unwrap` or one namespaced lookup away rather than a typed field.

## Alternatives considered

- **A wrapper per broker with no shared model.** Nothing to reuse across systems.
- **JMS or AMQP 1.0 as the abstraction.** Messaging only; no management, metrics or topology.
- **A schema-less map API.** Easy to add systems, impossible to program against with confidence.
