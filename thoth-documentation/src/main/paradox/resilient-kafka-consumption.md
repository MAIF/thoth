# Resilient kafka consumption 

Thoth provides a resilient kafka consumer. 

## Installation 

@@dependency[sbt,Maven,Gradle] {
    symbol="ThothVersion"
    value="$project.version.short$"
    group="fr.maif" artifact="thoth-kafka-consumer-reactor$" version="ThothVersion"
}

## Usage 

```java

ResilientKafkaConsumer<String, String> resilientKafkaConsumer = ResilientKafkaConsumer.create(
        // Name of the consumer (for logs etc ...)
        "test",
        ResilientKafkaConsumer.Config.create(
            List.of(topic),
            groupId,
            receiverOptions
        ),
        event -> {
            System.out.println(event.value());
        }
);
```

## Consuming event 

There 3 way to consume events : blocking, non-blocking, streams  

### Blocking 

It append when you a blocking call (i.e JDBC), in this case, a "parallel" scheduler is chosen.    

```java
ResilientKafkaConsumer<String, String> resilientKafkaConsumer = ResilientKafkaConsumer.create(
        // Name of the consumer (for logs etc ...)
        "test",
        ResilientKafkaConsumer.Config.create(
            List.of(topic),
            groupId,
            receiverOptions
        ),
        event -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
```

### Non Blocking 

```java
ResilientKafkaConsumer<String, String> resilientKafkaConsumer = ResilientKafkaConsumer.create(
        "test",
        ResilientKafkaConsumer.Config.create(
            List.of(topic),
            groupId,
            receiverOptions
        ),  
        // Non blocking handling 
        (ReceiverRecord<String, String> event) -> Mono.fromCallable(() -> {
            System.out.println(event.value());
            return event;
        })
);
```

### Streams

Stream handling is done using the reactor `Flux`. 

In that stream you can, skip, group etc do whatever you want but at the end you have to give back the event. 
 

```java
        ResilientKafkaConsumer.createFromFlow(
            "test",
            ResilientKafkaConsumer.Config.create(
                List.of(topic),
                groupId,
                receiverOptions
            ),
            flux -> flux
                .index()
                .concatMap(messageAndIndex -> {
                    Long index = messageAndIndex.getT1();
                    System.out.println("Message number " + index);
                    var event = messageAndIndex.getT2();
                    Mono<String> asyncApiCall = asyncApiCall();
                    return asyncApiCall.map(it -> event);
                })
        );
```


## Handling crash 

The goal of this consumer is to handle crashes by retrying the failed event consumption. There two types of errors :

 * Parsing / Business errors : in that case, you should push the failing event in a dead letter queue to analyse the issue and move to the next event.
 * Technical errors : e.g. a database or an API that is not available at time, in that case, you have to let it crash. 
   The consumer will let the event uncommitted, will disconnect from kafka and restart later, reading the message again.  
   
You can configure the consumer to set appropriate values for restart interval ... 

```java 
ResilientKafkaConsumer.Config.create(
                                List.of(topic),
                                groupId,
                                receiverOptions
                        )
                        .withCommitSize(5)
                        .withMinBackoff(Duration.ofMillis(200))
                        .withMaxBackoff(Duration.ofMinutes(10))
                        .withRandomFactor(0.2d);
```


## Status and lifecycles 

The resilient kafka consumer has a lifecycle and will have the following states : 

 * `Starting`: The consumer is starting 
 * `Started`: The consumer has started    
 * `Failed`: The consumer has crashed and will restart, the kafka client is no longer connected to the cluster. 
 * `Stopping` : The consumer is stopping   
 * `Stopped` : The consumer is stopped, the kafka client is no longer connected to the cluster. 

The status is exposed by the `ResilientKafkaConsumer` using the `status` method. 

```java
ResilientKafkaConsumer<String, String> resilientKafkaConsumer = ResilientKafkaConsumer.create(...);

Status status = resilientKafkaConsumer.status();
``` 


You can also register callbacks : 

```java
    ResilientKafkaConsumer<String, String> resilientKafkaConsumer = ResilientKafkaConsumer.create(
            ResilientKafkaConsumer.Config.create(
                List.of(topic),
                groupId,
                receiverOptions
            )
            .withOnStarting(() -> Mono.fromRunnable(() -> {
    
            }))
            .withOnStarted((c, time) -> Mono.fromRunnable(() -> {
    
            }))
            .withOnStopping(() -> Mono.fromRunnable(() -> {
    
            }))
            .withOnStopped(() -> Mono.fromRunnable(() -> {
    
            }))
            .withOnFailed(e -> Mono.fromRunnable(() -> {
    
            })), 
            event -> {
                names.set(names.get() + " " + event.record().value());
            }
    );
```