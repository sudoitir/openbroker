## 1. Catalog

- [x] 1.1 Add the schema catalog API, the registry connection options, and the capability on the probe.
- [x] 1.2 Reach a Confluent-compatible registry. A preview writes nothing. A stale etag is refused and writes nothing.
- [x] 1.3 Decode and encode Avro, Protobuf and JSON by registered name. A class name is not loaded.
- [x] 1.4 Prove it against Apicurio 3.3.5 beside the shared Kafka cluster, including a credential that never appears in an exception or a log line.
- [x] 1.5 Offer only `none` and `confluent` as registry kinds: a connection naming another is refused at the schema check. A Glue client is a later change, because its schema version id is a UUID the driver will not invent (ADR-0040).

## 2. Release

- [x] 2.1 The Release workflow ran for 0.13.0 after this merged, and Central serves it.
