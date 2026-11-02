## ADDED Requirements

### Requirement: Settings of a node or the cluster have a resource class
The SDK SHALL define `ResourceClass.CONFIGURATION` for the settings of a node or of the whole cluster, such as
Kafka broker and cluster-default configuration or RabbitMQ runtime parameters.

#### Scenario: Shared code reads broker settings
- **WHEN** a caller queries the class `CONFIGURATION` on a driver that declares such a kind
- **THEN** the driver returns the resources of its kinds of that class
