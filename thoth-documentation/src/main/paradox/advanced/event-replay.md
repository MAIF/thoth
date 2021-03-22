# Event replay 

In the case where kafka is not available, event can't be sent. 
In that case the module in charge to deliver events to kafka will crash and restart with an exponential backoff.

During the restart, in order to keep the order of the events, the new / current events are kept in memory while messages from the database are sent to kafka : 
 1. read events from DB where published is false
 2. send the events to kafka 
 3. send current events that were kept in a queue

If your application has more than 1 node and each nodes perform the same work, there is a problem because events will probably be sent twice or more. 
In that case you have to choose a strategy : 
 * `NO_STRATEGY` : you don't care and each node do the work
 * `SKIP` : one node replay the events and the others ignore events from DB. The issue with this strategy is that events which are newer than the replayed events can be sent from the other node. The order is not preserved.
 * `WAIT` : one node replay the events and the others wait and keep events in memory. The issue with this strategy is that if there a lot of write, the process will crash et retry a lot because the in memory queue will be full. 

You can configure this with the builder: 

```java
this.eventProcessor = ReactivePostgresKafkaEventProcessor
        .withSystem(actorSystem)
        .withPgAsyncPool(pgAsyncPool)
        .withTables(tableNames())
        .withTransactionManager()
        .withEventFormater(BankEventFormat.bankEventFormat.jacksonEventFormat())
        .withNoMetaFormater()
        .withNoContextFormater()
        .withKafkaSettings("bank", producerSettings(settings()))
        // Here you can choose withWaitConcurrentReplayStrategy, withSkipConcurrentReplayStrategy, withNoConcurrentReplayStrategy
        .withWaitConcurrentReplayStrategy()
        .withEventHandler(eventHandler)
        .withDefaultAggregateStore()
        .withCommandHandler(commandHandler)
        .withProjections(this.withdrawByMonthProjection)
        .build();
```