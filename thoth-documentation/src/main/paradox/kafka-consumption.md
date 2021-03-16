# Using published events

Thoth provides helpers to deserialize published events correctly.

## Event format

Published events aren't raw BankEvent, they are wrapped in an "Envelope" that contains the event and some meta-data.

Envelope class has the following fields:

* id: unique ID of the event (UUID)
* sequenceNum: incremental sequenceNum (sequence is common to all events)
* eventType: type of the event (MoneyWithdrawn, AccountOpened, ...)
* emissionDate: date of event emission
* metadata: JSON field that could contain additional metadata about the event
* event: event serialized as JSON
* context: JSON field that could contain additional context information about the event
* version: version of the event
* published: field used internally by thoth to indicate event publication, consumer shouldn't use it
* transactionId: id of the database transaction that created the event. It can be used to aggregate and consume at once every event of a given transaction (to ensure data consistency for instance).
* totalMessageInTransaction: number total of messages created during the database transaction, useful to aggregate and consume at once events of a transaction.
* numMessageInTransaction: indicate position of this event among other events created during the database transaction, useful to consume in the correct order all events created during a transaction. 
* entityId: id of the business entity concerned by this event 
* userId: id of the user that triggered event creation, useful for audit purposes
* systemId: id of the system that triggered event creation, useful for audit purposes


## Consume events

Thoth provides an helper to deserialize event value.

```java
Map<String, Object> props = new HashMap<>();

props.put(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
    bootstrapAddress);
props.put(
    ConsumerConfig.GROUP_ID_CONFIG,
    groupId);

KafkaConsumer<String, EventEnvelope<BankEvent, Tuple0, Tuple0>> consumer = new KafkaConsumer<>(
        props,
        new StringDeserializer(),
        JsonDeserializer.of(new BankEventFormat())
);
```