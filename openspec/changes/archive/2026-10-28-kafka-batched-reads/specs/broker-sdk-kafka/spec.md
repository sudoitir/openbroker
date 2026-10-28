## ADDED Requirements

### Requirement: Reads are batched per page
The driver SHALL serve a page of resources, and a read of several queries, with a number of Admin rounds that does not grow with the number of groups or topics in it: the groups' descriptions and offsets, the topics' partitions and their log offsets are each asked once for everything the page or the queries name. A query that fails SHALL fail alone, with every other query answered.

#### Scenario: A page of many subscriptions
- **WHEN** a consumer reads one query for each of twelve subscriptions, each naming its own group and topic
- **THEN** the driver asks for the group listing, the group descriptions, the groups' offsets, the topics' partitions and the log offsets in as many rounds as for two queries

#### Scenario: One query fails
- **WHEN** one of several queries names a topic that does not exist
- **THEN** the other queries are answered, and the failing query reports its own result
