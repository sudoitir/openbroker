## 1. Driver

- [x] 1.1 Refuse create and delete of an internal topic before any broker call.
- [x] 1.2 Preview create with validate-only, and refuse a bad partition count, replication factor or unknown setting first.
- [x] 1.3 Preview delete with the groups, the record estimate and ifEmpty, and refuse a stale etag.
- [x] 1.4 Preview a partition increase with validate-only, and refuse a shrink and a replica assignment.
- [x] 1.5 Stop honouring disconnectConsumers on group delete.
- [x] 1.6 The new tests pass on the ZooKeeper 2.8 image and on current KRaft.
