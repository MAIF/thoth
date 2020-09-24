# Event sourcing [![travis-badge][]][travis]

[travis]:               https://travis-ci.com/MAIF/scribe
[travis-badge]:         https://travis-ci.com/MAIF/java-eventsourcing.svg?token=yQytm3eoBniFj9mCoKpy&branch=master

This repository provide tools to do event sourcing in your application. 
 
This libs are based on : 
 * Vavr for functional stuff (immutable `List`, `Either`, `Future`)
 * Akka stream for reactive streams
 * jackson for json 
 * jooq to build query 
 * vertx for reactive postgresql database access  
 * Postgresql and kafka are the in production tested data stores 

## Modules 

 * `commons-event`: the pojos that represent the stored events. Can be used to parse events 
 * `eventsourcing-core`: The APIs for event sourcing 
 * `eventsourcing-jooq`: A jooq simple implementation of the `eventsourcing-core` APIs   
 * `eventsourcing-jooq-async`: A jooq implementation of the `eventsourcing-core` APIs using the `jooq-async-api`interface
 
## The things to know 

The vavr `Future` is used for async call (java `CompletionStage` is not user friendly). 

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

## Documentations 

* [Event sourcing](./eventsourcing-core/readme.md) : the documentation of the core components
* [Event sourcing with reactive postgresql](./eventsourcing-jooq-async/readme.md) : wire all together with the reactive postgresql client

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
