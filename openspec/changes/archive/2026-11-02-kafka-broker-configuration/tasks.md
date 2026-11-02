## 1. API
- [x] 1.1 Add `ResourceClass.CONFIGURATION`; handle it in every exhaustive switch

## 2. Kafka driver
- [x] 2.1 `BrokerConfigs` handler and the `kafka.broker-config` kind
- [x] 2.2 Read: one `describeConfigs` per page, `kafka.config_read_only`, `kafka.config_sources`
- [x] 2.3 Update: preview with etag, refusals before any write, `incrementalAlterConfigs`, settle
- [x] 2.4 Map `ClusterAuthorizationException` to `NOT_AUTHORIZED`

## 3. Tests
- [x] 3.1 `BrokerConfigsTest` against a mocked Admin
- [x] 3.2 `KafkaBrokerConfigsIT` against a real cluster
