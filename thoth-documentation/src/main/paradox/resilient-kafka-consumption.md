# Resilient kafka consumption 

Thoth provides a resilient kafka consumer. 

## Installation 

@@dependency[sbt,Maven,Gradle] {
    symbol="ThothVersion"
    value="$project.version$"
    group="fr.maif" artifact="thoth-kafka-goodies_2.13" version="ThothVersion"
}

## Usage 

```java

ResilientKafkaConsumer<String, String> resilientKafkaConsumer = ResilientKafkaConsumer.create(
        // Actor system
        system,
        // Name of the consumer (for logs etc ...)
        "MyConsumer",
        // Config 
        ResilientKafkaConsumer.Config.create(
                // Kafka subscription 
                Subscriptions.topics(topic),
                // Kafka group id 
                groupId,
                // The alpakka kafka consumer settings  
                ConsumerSettings
                        .create(system, new StringDeserializer(), new StringDeserializer())
                        .withBootstrapServers(bootstrapServers())
        ),
        // The event consumer, in that case the consumer print the events 
        event -> {
            System.out.println(event.record().value());
        }
);
```

## Consuming event 

There 3 way to consume events : blocking, non-blocking, streams  

### Blocking 

You don't need to perform IO or you have blocking IO e.g. JDBC.  

```java
ResilientKafkaConsumer<String, String> resilientKafkaConsumer = ResilientKafkaConsumer.create(
                system,
                "test",
                ResilientKafkaConsumer.Config.create(
                        Subscriptions.topics(topic),
                        groupId,
                        ConsumerSettings
                                .create(system, new StringDeserializer(), new StringDeserializer())
                                .withBootstrapServers(bootstrapServers())
                ),
                // Provide an executor 
                Executors.newCachedThreadPool(),
                // Blocking handling ! 
                event -> {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
        );
```

### Non Blocking 

```java
ResilientKafkaConsumer<String, String> resilientKafkaConsumer = ResilientKafkaConsumer.create(
                system,
                "test",
                ResilientKafkaConsumer.Config.create(
                        Subscriptions.topics(topic),
                        groupId,
                        ConsumerSettings
                                .create(system, new StringDeserializer(), new StringDeserializer())
                                .withBootstrapServers(bootstrapServers())
                ),
                // Non blocking handling 
                (CommittableMessage<String, String> event) -> CompletableFuture.supplyAsync(() -> {
                    System.out.println(event.record().value());
                    return Done.done();
                })
        );
```

### Streams

Stream handling is done using akka stream `Flow` which is a pipe that take a `ConsumerMessage.CommittableMessage<K, K>` in and should return a `ConsumerMessage.CommittableOffset`. 

In that stream you can, skip, group etc do whatever you want but at the end you have to provide the offset to commit. 
In order to have a better developer experience, you could use the `FlowWithContext` akka stream api if you don't want to have to handle the commit offset. 

With the classic flow api, you have to return the committable offset : 

```java
        ResilientKafkaConsumer.createFromFlow(
                system,
                "test",
                ResilientKafkaConsumer.Config.create(
                        Subscriptions.topics(topic),
                        groupId,
                        ConsumerSettings
                                .create(system, new StringDeserializer(), new StringDeserializer())
                                .withBootstrapServers(bootstrapServers())
                ),
                Flow.<CommittableMessage<String, String>>create()
                        .zipWithIndex()
                        .mapAsync(1, messageAndIndex -> {
                            Long index = messageAndIndex.second();
                            System.out.println("Message number " + index);
                            CommittableOffset committableOffset = messageAndIndex.first().committableOffset();
                            CompletionStage<Done> asyncApiCall = asyncApiCall();
                            return asyncApiCall.thenApply(__ -> committableOffset);
                        })
        );
```

With the `FlowWithContext` you don't : 

```java
ResilientKafkaConsumer.createFromFlowCtxAgg(
                system,
                "test",
                ResilientKafkaConsumer.Config.create(
                        Subscriptions.topics(topic),
                        groupId,
                        ConsumerSettings
                                .create(system, new StringDeserializer(), new StringDeserializer())
                                .withBootstrapServers(bootstrapServers())
                ),
                FlowWithContext.<CommittableMessage<String, String>, CommittableOffset>create()
                        .grouped(3)
                        .map(messages -> {
                            String collectedMessages = messages.stream().map(m -> m.record().value()).collect(Collectors.joining(" "));
                            names.set(collectedMessages);
                            return Done.done();
                        })
        );
```

If your `FlowWithContext` is doing aggregations like in this example (the `group` operator), you have to use `createFromFlowCtxAgg` instead of `createFromFlowCtx`.  

You'll find more information on the akka stream documentation https://doc.akka.io/docs/akka/current/stream/index.html. 


## Handling crash 

The goal of this consumer is to handle crashes by retrying the failed event consumption. There two types of errors :

 * Parsing / Business errors : in that case, you should push the failing event in a dead letter queue to analyse the issue and move to the next event.
 * Technical errors : e.g. a database or an API that is not available at time, in that case, you have to let it crash. 
   The consumer will let the event uncommitted, will disconnect from kafka and restart later, reading the message again.  
   
You can configure the consumer to set appropriate values for restart interval ... 

```java 
ResilientKafkaConsumer.Config.create(
                        Subscriptions.topics(topic),
                        groupId,
                        ConsumerSettings
                                .create(system, new StringDeserializer(), new StringDeserializer())
                                .withBootstrapServers(bootstrapServers())
                )
                        // Nb events before commit 
                        .withCommitSize(5)
                        // First delay for restart, it increase exponentially (200ms then 400ms then 800ms then 1600ms ...)                        
                        .withMinBackoff(Duration.ofMillis(200))
                        // Maximum restart delay
                        .withMaxBackoff(Duration.ofMinutes(10))
                        // Noise to restart non linearly 
                        .withRandomFactor(0.2d)
```


## Status and lifecycles 

The resilient kafka consumer has a lifecycle and will have the following states : 

 * `Starting`: The consumer is starting 
 * `Started`: The consumer has started and a `Control` object is available to "interact" with the kafka client   
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
            system,
            "test",
            ResilientKafkaConsumer.Config
                    .create(
                            Subscriptions.topics(topic),
                            groupId,
                            ConsumerSettings
                                    .create(system, new StringDeserializer(), new StringDeserializer())
                                    .withBootstrapServers(bootstrapServers())
                    )
                    .withOnStarting(() -> CompletableFuture.supplyAsync(() -> {
                        isStarting.set(true);
                        return Done.done();
                    }))
                    .withOnStarted((c, time) -> CompletableFuture.supplyAsync(() -> {
                        isStarted.set(true);
                        return Done.done();
                    }))
                    .withOnStopping(c -> CompletableFuture.supplyAsync(() -> {
                        isStopping.set(true);
                        return Done.done();
                    }))
                    .withOnStopped(() -> CompletableFuture.supplyAsync(() -> {
                        isStopped.set(true);
                        return Done.done();
                    }))
                    .withOnFailed(e -> CompletableFuture.supplyAsync(() -> {
                        isFailed.set(true);
                        return Done.done();
                    }))
            , event -> {
                names.set(names.get() + " " + event.record().value());
            }
    );
```