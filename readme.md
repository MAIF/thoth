# Event sourcing [![github-action-badge][]][github-action] [![jar-badge][]][jar]

[github-action]:        https://github.com/MAIF/thoth/actions?query=workflow%3ABuild
[github-action-badge]:  https://github.com/MAIF/thoth/workflows/Build/badge.svg?branch=master
[jar]:              https://maven-badges.herokuapp.com/maven-central/fr.maif/thoth-core_2.13
[jar-badge]:        https://maven-badges.herokuapp.com/maven-central/fr.maif/thoth-core_2.13/badge.svg

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

![](thoth-documentation/src/main/paradox/img/thoth_event_sourcing.jpg)

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

## Documentation

See our [documentation](https://maif.github.io/thoth/manual/).

## Limits

* A single command can't currently modify multiple entities [see this issue](https://github.com/MAIF/thoth/issues/4)

## Development 

### Compile / Test 

```bash
./gradlew compileJava
```

```bash
docker-compose -f docker-compose.test.yml up 
./gradlew test
```

### Generate the documentation 

```bash
cd thoth-documentation
sbt generateDoc
```
