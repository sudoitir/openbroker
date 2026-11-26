## 1. Catalog

- [x] 1.1 Add the schema catalog API, the registry connection options, and the capability on the probe.
- [x] 1.2 Reach a Confluent-compatible registry. A preview writes nothing. A stale etag is refused and writes nothing.
- [x] 1.3 Decode and encode Avro, Protobuf and JSON by registered name. A class name is not loaded.
- [x] 1.4 Prove it against Apicurio 3.3.5 beside the shared Kafka cluster, including a credential that never appears in an exception or a log line.
- [x] 1.5 Report Glue unavailable with its reason rather than inventing a schema id. Do not use the ambient credential chain.

## 2. Release

- [ ] 2.1 A maintainer runs the Release workflow after this merges. This change does not run it.
