# Event sourcing [![github-action-badge][]][github-action] [![jar-badge][]][jar]

[github-action]:        https://github.com/MAIF/thoth/actions?query=workflow%3ABuild
[github-action-badge]:  https://github.com/MAIF/thoth/workflows/Build/badge.svg?branch=master
[jar]:                  https://bintray.com/maif-functional-java/maven/thoth-core/_latestVersion
[jar-badge]:            https://api.bintray.com/packages/maif-functional-java/maven/thoth-core/images/download.svg

<p align="center">
    <img src="thoth.png" alt="thoth" width="300"/>
</p>

This repository provides tools to implement event sourcing in your application. 

It guaranties that:
* Events will be written in the database before being published in Kafka
* Publication in Kafka will be reattempted until it succeeds

It provides capabilities of defining two types of projections:
* "Transactional" projections, that are updated in the same transaction as the events
* "Eventually consistent" projections, updated asynchronously by consuming Kafka

![](doc/thoth_event_sourcing.jpg)

It also allows storing snapshots of the application state, for scenarios that implies lot of events. 

These libs are based on : 
 * Vavr for functional stuff (immutable `List`, `Either`, `Future`)
 * Akka stream for reactive streams
 * jackson for json 
 * jooq to build query 
 * vertx for reactive postgresql database access  
 * Postgresql and kafka are the in production tested data stores 

## Modules 

 * `commons-event`: POJOs that represent the stored events. Can be used by consumers to parse events. 
 * `thoth-core`: APIs for event-sourcing 
 * `thoth-jooq`: A jooq simple implementation of the `thoth-core` APIs   
 * `thoth-jooq-async`: A jooq implementation of the `thoth-core` APIs using the `jooq-async-api`interface
 
## Things to know 

The vavr `Future` is used for async call (java `CompletionStage` is not user-friendly). 

The akka stream `Source` is used for stream processing. 

The vavr `Either` is used to handle business errors. The idea is to have three channels:  
 * it's ok: `Future(Right("Result"))` 
 * it's an error: `Future(Left("Bad request"))`
 * it's a failure: `Future.failed(CrashedException("Crap!"))`

`io.vavr.Tuple0` is used instead of `void` so everything can be an expression: 

```java
Tuple0 sideEffect() {
    println("I have done a side effect");
    return Tuple.empty();
}
```

## Documentation

* [Event sourcing](./doc/banking.md): documentation of the core components, implementing a sample in-memory banking application
* [Jooq/Kafka](./doc/banking-real-life.md): migration of the sample application from in-memory to Postgres(JDBC) / Kafka
* [Projections](./doc/projections.md): projections documentation, implementing projection in sample application
* [Database configuration](./doc/database%20configuration.md): databases index to create
* [Messages](./doc/message.md): documentation of returning messages/warnings while handling commands
* [Custom event ordering](./doc/event-ordering.md): documentation on custom event ordering
* [Aggregate store](./doc/aggregatestore.md): documentation on periodic snapshot storing for performances
* [Non blocking Postgres / Kafka implementation](./doc/banking-real-life-non-blocking.md): documentation of non blocking postgres / Kafka implementation using reactive postgres vertx driver

## Limits

* A single command can't currently modify multiple entities [see this issue](https://github.com/MAIF/thoth/issues/4)

## Development 

### Compile / Test 

```bash
sbt compile
```

```bash
docker-compose -f docker-compose.test.yml up 
sbt test
```

Test rerun on each changes 

```bash
sbt ~test
```
