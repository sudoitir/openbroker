## 1. Guard the credential source
- [x] 1.1 Wrap the source `BrokerDrivers.connect` and `verify` hand a driver, reporting a non-`BrokerException` failure as `UNAVAILABLE`. Check: `CredentialSourceFailureTest`
- [x] 1.2 Verify: `./mvnw -pl openbroker-api test`; CI runs the TCK and japicmp
