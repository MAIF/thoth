# Words on API 

Thoth is non-blocking by default and rely on vavr but, you can choose if you use it or not.   



## Basic API

The basic API use vavr for several reasons : 
* `Either` : to handle success or error 
* `Option` : instead of java Optional to handle missing values 
* `Tuple` : to collect data
* `Tuple0` : to return values not handled (like Void but not exactly the same)
* `List` : instead of java List, because the API is better 
* `Lazy` : for lazy values 

The basic API, is exposed with package `fr.maif.eventsourcing`.  

## Vanilla API 

If you don't want to use vavr, there is a vanilla API : 
* `Either` -> `fr.maif.eventsourcing.Result`: a custom type to handle success or error
* `Option` -> `Optional` 
* `Tuple` : internal 
* `Tuple0` -> `fr.maif.eventsourcing.Unit` : a custom type to return values not handled (like Void but not exactly the same)
* `List` : -> `java.util.List`  
* `Lazy` : for lazy values

The package to use is `fr.maif.eventsourcing.vanilla` :
* `AggregateStore`
* `CommandHandler`
* `EventHandler`
* `EventProcessor`
* `EventPublisher`
* `Events`
* `EventStore`
* `ProcessingSuccess`
* `Projection`
* `SimpleCommand`


## Blocking API 

If you don't to handle the non-blocking aspect, you can implement blocking interface, ie the blocking `CommandHandler`. 

The component to use 
* `fr.maif.eventsourcing.blocking.CommandHandler` : with standard API (vavr)
* `fr.maif.eventsourcing.vanilla.blocking.CommandHandler` : with vanilla API

## Reactive API using reactor  

There is a reactor API to use `Mono` instead of `CompletionStage`. The classes are prefixed with `Reactor` : 
* `ReactorAggregateStore`
* `ReactorCommandHandler`
* `ReactorEventProcessor`
* `ReactorEventStore`
* `ReactorPostgresKafkaEventProcessorBuilder`
* `ReactorProjection`
* `ReactorTransactionManager`

At the moment, the reactor API use vavr, there is no vanilla API. 