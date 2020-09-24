# Event sourcing [![travis-badge][]][travis]

[travis]:               https://travis-ci.com/MAIF/java-eventsourcing
[travis-badge]:         https://travis-ci.com/MAIF/java-eventsourcing.svg?token=yQytm3eoBniFj9mCoKpy&branch=master

This repository provide several tools to 
 * do event sourcing
 * interact with databases
 * do validation 
 * manipulate json

This libs are based on : 
 * Vavr for functional stuff (immutable `List`, `Either`, `Future`)
 * Akka streaming for reactive streams
 * jackson for json 
 * jooq to build query 
 * vertx for reactive postgresql database access  
 * Postgresql and kafka are the in production tested data stores 

## Modules 

 * `commons-event`: the pojos that represent the stored events. Can be used to parse events 
 * `eventsourcing-core`: The APIs for event sourcing 
 * `eventsourcing-jooq`: A jooq simple implementation of the `eventsourcing-core` APIs   
 * `eventsourcing-jooq-async`: A jooq implementation of the `eventsourcing-core` APIs using the `jooq-async-api`interface
 * `jooq-async-api`: An API interface to use jooq as a query builder for reactive database libraries   
 * `jooq-async-jdbc`: A `jooq-async-api` implementation using the blocking jdbc. 
 * `jooq-async-reactive`: A `jooq-async-api` implementation using vertx reactive postgresql client  
 * `jooq-async-api-tck`: A test kit to test an `jooq-async-api` implementation
 * `json`: An API to read, write and manipulate Jackson Json classes 
 * `validations`: An API to write validate and combine and stack the results  

## The things to know 

The vavr `Future` is used for async call (java `CompletionStage` is not user friendly). 

The akka stream `Source` is used for stream processing. 

The vavr `Either` is used to handle business errors. The idea is to have three channels:  
 * it's ok: `Future(Right("Result"))` 
 * it' an error: `Future(Left("Bad request"))`
 * it' a failure: `Future.failed(CrashedException("Crap!"))`

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
* [Reactive Jooq](./jooq-async-api/readme.md) : The vertx reactive client with jooq query builder 
* [Validation](./validations/readme.md) : `Rule` library
* [Json](./json/readme.md) : `Json` library

## Development 

### Compile / Test 

```bash
sbt compile
```

```bash
sbt test
```
Test rerun on each changes 

```bash
sbt ~test
```

### Release

```bash
EXPORT BINTRAY_PASS=xxxx
EXPORT BINTRAY_USER=xxxx
```

```bash
sbt "release with-defaults"
```

```bash
sbt "release release-version 1.0.0.beta1 next-version 1.0.0-SNAPSHOT"
```