## 1. Declare and prove it
- [x] 1.1 Add `releaseCountsDelivery` to `SubscriptionCapabilities`; Kafka declares it true for share groups and false for group subscriptions, Artemis false. Check: api and kafka unit tests
- [x] 1.2 TCK case `anUncountedReleaseCountsAsADeliveryOnlyWhereDeclared`, and a share-group IT. Check: the messaging TCK on Kafka 4.4.0 and Artemis
- [x] 1.3 Raise the poms to `0.6.0-SNAPSHOT` and `openbroker.previous.version` to `0.5.0`. Check: build
