## ADDED Requirements

### Requirement: Broker configuration is a resource
The Kafka driver SHALL declare `kafka.broker-config`, of class `CONFIGURATION` with the verbs `LIST`, `GET` and
`UPDATE`. It SHALL list `*` for the cluster-wide default and one resource per broker in the cluster's metadata, and
SHALL read all configurations of a page with one `describeConfigs`.

#### Scenario: Listing a cluster of two brokers
- **WHEN** broker configurations are listed
- **THEN** `*`, `1` and `2` are returned, read with a single `describeConfigs`

### Requirement: Broker settings are attributes, without secrets
Settings SHALL be attributes named `kafka.config.<name>`, and SHALL never include a sensitive setting.
`kafka.config_read_only` SHALL name the settings only a restart changes, and `kafka.config_sources` SHALL name, per
overridden setting, `node`, `cluster` or `static`.

#### Scenario: A secret setting
- **WHEN** a broker has a sensitive setting
- **THEN** no attribute carries it

### Requirement: A dynamic broker setting is updated safely
`UPDATE` SHALL preview the before and after of the patched settings with an etag, and SHALL apply them with
`incrementalAlterConfigs`: `SET`, or `DELETE` when the value is empty. It SHALL wait until the change shows. It
SHALL refuse a setting that only a restart changes with `FAILED_PRECONDITION` and `IMMUTABLE_FIELD`, saying to set
it in `server.properties` and restart the broker, and a sensitive setting with `INVALID_ARGUMENT`, in both cases
without writing and without quoting the value.

#### Scenario: Changing the cluster default
- **WHEN** `message.max.bytes` of `*` is updated and read back
- **THEN** the new value shows with source `cluster`, and an empty value removes the override

#### Scenario: A setting that needs a restart
- **WHEN** an update names `log.dirs`
- **THEN** it is refused with `IMMUTABLE_FIELD` and nothing is written

### Requirement: A cluster-authorization refusal names what is needed
The driver SHALL report `ClusterAuthorizationException` as `PERMISSION_DENIED` with reason `NOT_AUTHORIZED`.

#### Scenario: An account without cluster rights
- **WHEN** the cluster refuses an update for lack of cluster authorization
- **THEN** the failure is `PERMISSION_DENIED` / `NOT_AUTHORIZED`
