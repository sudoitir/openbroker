# broker-sdk-rabbitmq Specification

## Purpose
Defines the RabbitMQ driver: how virtual hosts, exchanges, the three queue types, bindings, shovels, federation links, policies and permissions map onto the SDK's resource model, why browse is offered only on streams, how sends report a message routed nowhere, and how the driver proves it against the TCK.

## Requirements

### Requirement: RabbitMQ is modelled honestly
The RabbitMQ driver SHALL map virtual hosts to namespaces, exchanges to destinations, classic, quorum and stream
queues to subscriptions (one kind per queue type), and bindings, exchange-to-exchange bindings, shovels and
federation links to routes. It SHALL map policies to policies and permissions to access rules. Quorum queue
members SHALL appear as leader and follower placements, each marked in sync or not.

#### Scenario: A quorum queue's members
- **WHEN** a quorum queue runs on three nodes
- **THEN** its placements name the leader and the two followers

#### Scenario: A stream cannot be purged
- **WHEN** the kinds of a RabbitMQ cluster are listed
- **THEN** the stream kind offers no purge verb, and the classic and quorum kinds do

### Requirement: Browse never consumes
The driver SHALL offer browse only where it is non-destructive (stream queues). It SHALL declare browse
unavailable for classic and quorum queues rather than get and requeue messages, and SHALL make no broker call
when refusing it.

#### Scenario: Browsing a classic queue
- **WHEN** a caller asks to browse a classic queue
- **THEN** the driver refuses with `NOT_SUPPORTED` and a reason, and no message is fetched

### Requirement: A send routed nowhere is reported
The driver SHALL publish with publisher confirms and `mandatory`, and SHALL fail a send that the broker returned
unroutable with `NOT_ROUTED`.

#### Scenario: Publishing to an exchange with no binding
- **WHEN** a message is sent to an exchange no queue is bound to
- **THEN** the send fails with `NOT_ROUTED`

### Requirement: The RabbitMQ driver passes the TCK
The driver SHALL pass every TCK case for the capabilities it declares, against a real RabbitMQ cluster, for
classic, quorum and stream queues.

#### Scenario: CI runs the TCK
- **WHEN** the driver's integration tests run on each supported image
- **THEN** every declared capability's TCK cases pass
