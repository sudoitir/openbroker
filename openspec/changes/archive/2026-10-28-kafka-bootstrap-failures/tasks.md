## 1. Classify before connecting
- [x] 1.1 `Sockets` and `Bootstrap` resolve and connect to each bootstrap server; `KafkaCluster` makes the Admin client from the servers that answer. Check: unit tests
- [x] 1.2 `KafkaErrors` maps a client that could not be built for want of a resolvable server. Check: unit test
- [x] 1.3 Reconnect backoff in the client settings. Check: unit test
- [x] 1.4 TCK case `verifyClassifiesAnUnreachableAddress` and `TckEnvironment.requestToAddress`. Check: Kafka and Artemis TCKs
- [x] 1.5 Poms to `0.6.2-SNAPSHOT`, previous version `0.6.1`. Check: build
- [x] 1.6 The `kafka.hanging-transaction` kind: classification, listing, preview and abort. Check: unit tests and `HangingTransactionsIT`
