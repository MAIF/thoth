# Getting started

Thoth is published on maven central.

Thoth is reactive on its core, but it could be used is a blocking context.

## Modules

### Commons events

Basic interface concerning events. This module is required

@@dependency[sbt,Maven,Gradle] {
symbol="ThothVersion"
value="$project.version.short$"
group="fr.maif"
artifact="commons-events$"
version="ThothVersion"
}

### Thoth core

Main interfaces and implementations. This module is required.

@@dependency[sbt,Maven,Gradle] {
symbol="ThothVersion"
value="$project.version.short$"
group="fr.maif"
artifact="thoth-core$"
version="ThothVersion"
}

### Thoth core-akka, thoth-jooq-akka and thoth-kafka-consumer-akka

Those modules are not maintained anymore since the version switch of akka.

### Thoth core reactor

Basically, this module is used to garanti the publication of events in kafka (outbox pattern).
Unless you bring your own implementation, this module is required.
It uses reactor, but the reactor API is not exposed.

@@dependency[sbt,Maven,Gradle] {
symbol="ThothVersion"
value="$project.version.short$"
group="fr.maif"
artifact="thoth-reactor$"
version="ThothVersion"
}

### Jdbc (Thoth jooq)

A jooq + jdbc implémentation of the event store.

If your app is running on a servlet context, you should use this module.
Jooq is used on the inner implementation but, it's not required for your business code.

@@dependency[sbt,Maven,Gradle] {
    symbol="ThothVersion"
    value="$project.version.short$"
    group="fr.maif"
    artifact="thoth-jooq$"
    version="ThothVersion"
}

### Reactive (Thoth jooq reactor)

A jooq with jdbc or vertx-sql implémentation of the event store.

This module is based on https://github.com/MAIF/jooq-async to access database.
Jooq async provided a jdbc (thread pool based) and vertx implémentation (reactive) at the moment.

If your app is running on a reactive context, you should use this module.
Jooq is used on the inner implementation but, it's not required for your business code.

@@dependency[sbt,Maven,Gradle] {
    symbol="ThothVersion"
    value="$project.version.short$"
    group="fr.maif"
    artifact="thoth-jooq-reactor$"
    version="ThothVersion"
}

### thoth-kafka-consumer-reactor

A helper module to consume kafka topics. This module handle for you retries, concurrent consumption by partition etc

@@dependency[sbt,Maven,Gradle] {
    symbol="ThothVersion"
    value="$project.version.short$"
    group="fr.maif"
    artifact="thoth-kafka-consumer-reactor$"
    version="ThothVersion"
}

## Summary by usages

| Context                           | Dependencies                                                                |
|-----------------------------------|-----------------------------------------------------------------------------|
| Servlet / Blocking / Thread based | commons-events<br/>thoth-core<br/>thoth-core-reactor<br/>thoth-jooq         |
| Reactive                          | commons-events<br/>thoth-core<br/>thoth-core-reactor<br/>thoth-jooq-reactor |
