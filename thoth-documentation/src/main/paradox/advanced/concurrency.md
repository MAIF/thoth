# Handling concurrency in Thoth

There is two main ways to handle concurrency in Thoth:
* using the sequence num : you can send sequence number in your commands and raise an error in the command handler if the sequence number is lower than the current one
* using a locking mechanism : You can configure the aggregate with a lock option 

## The lock option provided by Thoth

The lock option is base on the "select for update" feature provided by postgresql. 

There is 3 options available:

* NO_STRATEGY : this is the default option, no lock is used
* WAIT_ON_LOCK : the concurrent command will wait for the lock to be released before executing
* FAIL_ON_LOCK : the concurrent command will fail if the lock is already taken

Exemple : 

```java
this.eventProcessor = ReactivePostgresKafkaEventProcessor
        .withPgAsyncPool(pgAsyncPool)
        .withTables(tableNames())
        .withTransactionManager()
        .withEventFormater(BankEventFormat.bankEventFormat.jacksonEventFormat())
        .withNoMetaFormater()
        .withNoContextFormater()
        .withKafkaSettings("bank", producerSettings(settings()))
        .withWaitConcurrentReplayStrategy()
        .withEventHandler(eventHandler)
        // Set the concurrency strategy for the aggregate store : 
        .withDefaultAggregateStore(ReadConcurrencyStrategy.WAIT_ON_LOCK)
        .withCommandHandler(commandHandler)
        .withProjections(this.withdrawByMonthProjection)
        .build();
```
