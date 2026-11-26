## ADDED Requirements

### Requirement: A cluster can have one schema registry

The driver SHALL accept one schema registry on a Kafka connection, a Confluent-compatible registry, or none. It SHALL list subjects, versions and compatibility in that vocabulary. An operation that registry does not offer SHALL be unavailable with that registry's reason, and the subject list SHALL still answer. With no registry configured, the capability SHALL be unavailable with the reason, and the driver SHALL NOT invent subjects. A registry kind other than none or confluent SHALL be refused with the reason. Credentials SHALL be asked for on each connection and SHALL NOT be returned on a read.

#### Scenario: A Confluent-compatible registry

- **WHEN** a caller lists subjects on a connection with a Confluent-compatible registry that has one subject
- **THEN** that subject is returned with its versions and compatibility

#### Scenario: No registry is configured

- **WHEN** a caller reads the registry capability on a connection with no registry
- **THEN** the capability is unavailable with the reason, and no subject is invented

#### Scenario: A kind this driver does not speak

- **WHEN** a caller sets the registry kind to a value other than none or confluent
- **THEN** the connection is refused with the reason

#### Scenario: The registry does not offer an operation

- **WHEN** a caller attempts an operation the chosen registry does not offer
- **THEN** that operation is unavailable with the registry's reason, and the subject list is still available

#### Scenario: A credential never appears

- **WHEN** a registry call fails and the connection was given a credential
- **THEN** the credential is absent from the exception message and from the log line

### Requirement: Registration, compatibility and delete are previewed

The driver SHALL preview a schema registration, a compatibility change and a subject delete. The preview SHALL show the subject before and after, SHALL say that nothing has been written, and SHALL carry an etag over the subject, its versions and its compatibility. Where the registry can judge compatibility without writing, the preview SHALL include that result. Where it cannot, the preview SHALL say that the compatibility check happens on apply, and SHALL still write nothing. A schema the registry already has SHALL be stated with its existing id, and applying that preview SHALL NOT create a second version. An apply whose etag no longer matches SHALL be refused and change nothing.

Where the registry distinguishes a recoverable delete from a permanent one, the permanent delete SHALL be its own preview and SHALL say that the schemas cannot be restored.

#### Scenario: A registration is only a preview

- **WHEN** a caller previews registering a new schema for a subject and does not apply it
- **THEN** the preview names the subject and the schema type, and the subject has no new version

#### Scenario: The schema is already registered

- **WHEN** a caller previews a schema the registry already has
- **THEN** the preview states the existing id, and applying it does not create a second version

#### Scenario: A version arrived after the preview

- **WHEN** another version is registered for the subject between the preview and the apply
- **THEN** the apply is refused as stale and the subject is unchanged

#### Scenario: A permanent delete

- **WHEN** a caller previews a permanent delete on a registry that distinguishes one
- **THEN** the preview says the schemas cannot be restored, and nothing is deleted until that preview is applied

#### Scenario: A permanent-delete preview writes nothing

- **WHEN** a caller previews a permanent delete and does not apply it
- **THEN** the subject and its versions are unchanged

### Requirement: Keys and values decode to the record

The driver SHALL decode a key or value that carries a schema id into the record, for Avro, Protobuf and JSON Schema. It SHALL encode a record with the same serde when a caller produces one, so that reading it back shows the record. A custom serde SHALL be a named binding on a topic's key or value and SHALL NOT replace those three types. A key or value with no schema, an id the registry does not have, or a serde the installation cannot run SHALL be returned undecoded with the reason, and the driver SHALL NOT invent fields.

#### Scenario: A record with a schema id

- **WHEN** a caller browses a value that carries a schema id the registry has
- **THEN** the value is the decoded record

#### Scenario: An unknown schema id

- **WHEN** a caller browses a value whose schema id the registry does not have
- **THEN** the value is undecoded with the reason, and no fields are invented

#### Scenario: A produced record reads back

- **WHEN** a caller produces a record with a subject and a serde, and then browses it
- **THEN** the browsed value is that record

#### Scenario: A custom serde cannot run

- **WHEN** a topic's value is bound to a custom serde the installation cannot run
- **THEN** the value is undecoded with the reason

#### Scenario: A serde named as a Java class is refused

- **WHEN** a caller decodes a payload with a serde named as a Java class
- **THEN** nothing is decoded and the reason is that the serde is not registered

#### Scenario: A tombstone has no schema id

- **WHEN** a caller decodes a tombstone
- **THEN** the value is undecoded with the reason no-schema-id, and it is not an empty record
