# Thoth


@@@ index

* [Getting started and modules](getting-started.md)
* [Technical considerations](technical-considerations.md)
* [In memory example](banking.md)
* [Standard implementation](standard/index.md)
* [Database configuration](database-configuration.md)
* [Advanced use cases](advanced/index.md)
* [Eventually consistent projections](eventually-consistent-projection.md)
* [Kafka consumption](kafka-consumption.md)
* [Non blocking implementation](non-blocking/index.md)
* [Resilient kafka consumption](resilient-kafka-consumption.md)
@@@ 

Thoth is a library that provides a way to implement event-sourcing in Java applications.

## Event sourcing

Rather than maintaining an up-to-date application state, event sourcing focuses on what happened by storing events.

This approach provides by-design audit log for the application.

It is also very well suited for event-driven architectures : events can be published as is.

It plays well with CQRS : building and maintaining read projections is done by consuming events.

## Thoth

Thoth guaranties that:

* Events will be written in the database before being published in Kafka (to prevent failure)
* Publication in Kafka will be reattempted until it succeeds

It provides capabilities of defining two types of projections :

* “Real time” projections, that are updated in the same transaction as the events
* “Eventually consistent” projections, updated asynchronously by consuming Kafka

![](img/thoth_event_sourcing.jpg)

## Documentation

This documentation focuses on implementing event-sourcing on a simple use case : a banking application.


## Installation 

@@dependency[sbt,Maven,Gradle] {
    symbol="ThothVersion"
    value="$project.version.short$"
    group="fr.maif" artifact="commons-events" version="ThothVersion"
    group2="fr.maif" artifact2="thoth-core-akka" version2="ThothVersion"
    group3="fr.maif" artifact3="thoth-core-reactor" version3="ThothVersion"
    group4="fr.maif" artifact4="thoth-jooq" version4="ThothVersion"
    group5="fr.maif" artifact5="thoth-jooq-akka" version5="ThothVersion"
    group6="fr.maif" artifact6="thoth-kafka-consumer-akka" version6="ThothVersion"
    group7="fr.maif" artifact7="thoth-kafka-consumer-reactor" version7="ThothVersion"
}

More détail [here](getting-started.md).

@@toc { depth=2 }
