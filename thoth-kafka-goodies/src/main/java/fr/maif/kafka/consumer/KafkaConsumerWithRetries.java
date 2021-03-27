package fr.maif.kafka.consumer;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.kafka.AutoSubscription;
import akka.kafka.CommitterSettings;
import akka.kafka.ConsumerMessage;
import akka.kafka.ConsumerSettings;
import akka.kafka.javadsl.Committer;
import akka.kafka.javadsl.Consumer;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.RestartSource;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static akka.Done.done;

@Slf4j
public abstract class KafkaConsumerWithRetries<K, V> {

    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Config<K, V> {
        public final AutoSubscription subscription;
        public final String groupId;
        public final ConsumerSettings<K, V> consumerSettings;
        public final Duration minBackoff;
        public final Duration maxBackoff;
        public final Integer randomFactor;
        public final Integer commitSize;
        public final java.util.function.Consumer<Consumer.Control> onStarted;
        public final Runnable onStarting;
        public final Runnable onStopped;
        public final java.util.function.Consumer<Consumer.Control> onStopping;
        public final java.util.function.Consumer<Throwable> onFailed;

        public static <K, V> Config<K, V> create(AutoSubscription subscription, String groupId, ConsumerSettings<K, V> consumerSettings) {
            return Config.<K, V>builder()
                    .consumerSettings(consumerSettings)
                    .groupId(groupId)
                    .subscription(subscription)
                    .build();
        }
    }
    protected final ActorSystem actorSystem;
    protected final Materializer materializer;

    protected final AutoSubscription subscription;
    protected final String groupId;
    protected final Duration minBackoff;
    protected final Duration maxBackoff;
    protected final double randomFactor;
    protected final Integer commitSize;
    protected final ConsumerSettings<K, V> consumerSettings;
    protected final java.util.function.Consumer<Consumer.Control> onStarted;
    protected final Runnable onStarting;
    protected final Runnable onStopped;
    protected final java.util.function.Consumer<Consumer.Control> onStopping;
    protected final java.util.function.Consumer<Throwable> onFailed;

    protected final AtomicReference<Consumer.Control> controlRef = new AtomicReference<>();
    protected final AtomicReference<Status> innerStatus = new AtomicReference<>(Status.stopped);

    public KafkaConsumerWithRetries(ActorSystem actorSystem, Config<K, V> config) {
        this.actorSystem = actorSystem;
        this.materializer = Materializer.createMaterializer(actorSystem);
        this.subscription = config.subscription;
        this.groupId = config.groupId;
        this.minBackoff = Objects.isNull(config.minBackoff) ? Duration.ofSeconds(30) : config.minBackoff;
        this.maxBackoff = Objects.isNull(config.maxBackoff) ? Duration.ofMinutes(30) : config.maxBackoff;
        this.randomFactor = Objects.isNull(config.randomFactor) ? 0.2d : config.randomFactor;
        this.commitSize = Objects.isNull(config.commitSize) ? 10 : config.commitSize;
        this.consumerSettings = config.consumerSettings;
        this.onStarted = defaultIfNull(config.onStarted, (__) -> {});
        this.onStarting = defaultIfNull(config.onStarting, () -> {});
        this.onStopped = defaultIfNull(config.onStopped, () -> {});
        this.onStopping = defaultIfNull(config.onStopping, (__) -> {});
        this.onFailed = defaultIfNull(config.onFailed, (__) -> {});
        this.start();
    }

    public static <K, V> KafkaConsumerWithRetries<K, V> create(ActorSystem actorSystem,
                                                               String name,
                                                               Config<K, V> config,
                                                               Flow<ConsumerMessage.CommittableMessage<K, V>, ConsumerMessage.CommittableOffset, NotUsed> messageHandling) {
        return new KafkaConsumerWithRetries<K, V>(actorSystem, config) {
            @Override
            protected String name() {
                return name;
            }

            @Override
            public Flow<ConsumerMessage.CommittableMessage<K, V>, ConsumerMessage.CommittableOffset, NotUsed> messageHandling() {
                return messageHandling;
            }
        };
    }

    public static <K, V> KafkaConsumerWithRetries<K, V> create(ActorSystem actorSystem,
                                                               String name,
                                                               Config<K, V> config,
                                                               Function<ConsumerMessage.CommittableMessage<K, V>, CompletionStage<Done>> handleMessage) {
        return new KafkaConsumerWithRetries<K, V>(actorSystem, config) {
            @Override
            protected String name() {
                return name;
            }

            @Override
            public Flow<ConsumerMessage.CommittableMessage<K, V>, ConsumerMessage.CommittableOffset, NotUsed> messageHandling() {
                return Flow.<ConsumerMessage.CommittableMessage<K, V>>create()
                        .flatMapConcat(m ->
                                Source.completionStage(handleMessage.apply(m))
                                .map(__ -> m.committableOffset())
                        );
            }
        };
    }

    public static <K, V> KafkaConsumerWithRetries<K, V> create(ActorSystem actorSystem,
                                                               String name,
                                                               Config<K, V> config,
                                                               Executor executor,
                                                               java.util.function.Consumer<ConsumerMessage.CommittableMessage<K, V>> handleMessage) {
        return new KafkaConsumerWithRetries<K, V>(actorSystem, config) {
            @Override
            protected String name() {
                return name;
            }

            @Override
            public Flow<ConsumerMessage.CommittableMessage<K, V>, ConsumerMessage.CommittableOffset, NotUsed> messageHandling() {
                return Flow.<ConsumerMessage.CommittableMessage<K, V>>create()
                        .flatMapConcat(m ->
                                Source.completionStage(CompletableFuture.supplyAsync(() -> {
                                    handleMessage.accept(m);
                                    return Done.done();
                                }, executor))
                                .map(__ -> m.committableOffset())
                        );
            }
        };
    }

    public static <K, V> KafkaConsumerWithRetries<K, V> create(ActorSystem actorSystem,
                                                               String name,
                                                               Config<K, V> config,
                                                               java.util.function.Consumer<ConsumerMessage.CommittableMessage<K, V>> handleMessage) {
        return create(actorSystem, name, config, Executors.newCachedThreadPool(), handleMessage);
    }

    protected static <T> T defaultIfNull(T v, T defaultValue) {
        if (Objects.isNull(v)) {
            return defaultValue;
        } else {
            return v;
        }
    }

    protected abstract String name();
    protected abstract Flow<ConsumerMessage.CommittableMessage<K, V>, ConsumerMessage.CommittableOffset, NotUsed> messageHandling();


    protected Logger logger() {
        return log;
    }

    private Status updateStatus(Status status) {
        innerStatus.set(status);
        return status;
    }

    public Status status() {
        return innerStatus.get();
    }

    public Status start() {
        Status currentStatus = status();
        if (Status.starting.equals(currentStatus) || Status.started.equals(currentStatus)) {
            logger().info("{} already started", name());
            return currentStatus;
        }
        updateStatus(Status.starting);
        CommitterSettings committerSettings = CommitterSettings.create(actorSystem);

        logger().info("Starting {} {} on topic '{}' with group id '{}'", name(), Integer.toHexString(this.hashCode()), subscription, groupId);

        RestartSource.onFailuresWithBackoff(
                minBackoff,
                maxBackoff,
                randomFactor,
                () -> {
                    this.onStarting.run();
                    logger().info("Stream for {} is starting", name());
                    return Consumer
                            .committablePartitionedSource(consumerSettings, subscription)
                            .flatMapMerge(100, tuple ->
                                    tuple.second()
                                            .via(messageHandling())
                                            .via(Committer.flow(committerSettings.withMaxBatch(100)))
                            )
                            .mapMaterializedValue(control -> {
                                updateStatus(Status.started);
                                this.onStarted.accept(control);
                                logger().info("Stream for {} has started", name());
                                controlRef.set(control);
                                return control;
                            })
                            .watchTermination((___, done) -> handleTerminaison(done));
                })
                .watchTermination((___, done) -> handleTerminaison(done))
                .runWith(Sink.ignore(), this.materializer);
        return Status.starting;
    }

    public CompletionStage<Done> stop() {
        updateStatus(Status.stopping);
        this.onStopping.accept(controlRef.get());
        return stopConsumingKafka()
                .whenComplete((__, ___) -> {
                    this.onStopped.run();
                    updateStatus(Status.stopped);
                });
    }

    protected CompletionStage<Status> handleTerminaison(CompletionStage<Done> done) {
        return done.toCompletableFuture()
                .thenApply(any -> {
                    logger().info("Stopping {}", name());
                    this.onStopped.run();
                    return updateStatus(Status.stopped);
                })
                .exceptionally(e -> {
                    logger().error("Error during " + name(), e);
                    this.onFailed.accept(e);
                    return updateStatus(Status.failed);
                })
                .thenCompose(status -> stopConsumingKafka().thenApply(any -> status))
                .toCompletableFuture();
    }


    protected CompletionStage<Done> stopConsumingKafka() {
        Consumer.Control control = controlRef.getAndSet(null);
        if (control != null) {
            return control.shutdown()
                    .whenComplete((d, e) -> {
                        if (e != null) {
                            logger().error("Error shutting down kafka consumer for {}", name());
                        } else {
                            logger().info("Kafka consumer for {} is shutdown", name());
                        }
                    })
                    .thenCompose(___ -> control.isShutdown())
                    .thenApply(__ -> done());
        } else {
            return CompletableFuture.completedFuture(done());
        }
    }
}
