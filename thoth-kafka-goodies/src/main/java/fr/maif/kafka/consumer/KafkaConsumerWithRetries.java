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
import lombok.With;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static akka.Done.done;
import static java.util.function.Function.identity;

@Slf4j
public abstract class KafkaConsumerWithRetries<K, V> {

    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @With
    public static class Config<K, V> {
        public final AutoSubscription subscription;
        public final String groupId;
        public final ConsumerSettings<K, V> consumerSettings;
        public final Duration minBackoff;
        public final Duration maxBackoff;
        public final Integer randomFactor;
        public final Integer commitSize;
        public final Function<Consumer.Control, CompletionStage<Done>> onStarted;
        public final Supplier<CompletionStage<Done>> onStarting;
        public final Supplier<CompletionStage<Done>> onStopped;
        public final Function<Consumer.Control, CompletionStage<Done>> onStopping;
        public final Function<Throwable, CompletionStage<Done>> onFailed;

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
    protected final Function<Consumer.Control, CompletionStage<Done>> onStarted;
    protected final Supplier<CompletionStage<Done>> onStarting;
    protected final Supplier<CompletionStage<Done>> onStopped;
    protected final Function<Consumer.Control, CompletionStage<Done>> onStopping;
    protected final Function<Throwable, CompletionStage<Done>> onFailed;

    protected final AtomicReference<Consumer.Control> controlRef = new AtomicReference<>();
    protected final AtomicReference<Status> innerStatus = new AtomicReference<>(Status.Stopped);

    public KafkaConsumerWithRetries(ActorSystem actorSystem, Config<K, V> config) {
        this.actorSystem = actorSystem;
        this.materializer = Materializer.createMaterializer(actorSystem);
        this.subscription = config.subscription;
        this.groupId = config.groupId;
        this.minBackoff = Objects.isNull(config.minBackoff) ? Duration.ofSeconds(30) : config.minBackoff;
        this.maxBackoff = Objects.isNull(config.maxBackoff) ? Duration.ofMinutes(30) : config.maxBackoff;
        this.randomFactor = Objects.isNull(config.randomFactor) ? 0.2d : config.randomFactor;
        this.commitSize = Objects.isNull(config.commitSize) ? 10 : config.commitSize;
        this.consumerSettings = config.consumerSettings
                .withGroupId(config.groupId)
                .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.onStarted = defaultIfNull(config.onStarted, (__) -> CompletableFuture.completedFuture(done()));
        this.onStarting = defaultIfNull(config.onStarting, () -> CompletableFuture.completedFuture(done()));
        this.onStopped = defaultIfNull(config.onStopped, () -> CompletableFuture.completedFuture(done()));
        this.onStopping = defaultIfNull(config.onStopping, (__) -> CompletableFuture.completedFuture(done()));
        this.onFailed = defaultIfNull(config.onFailed, (__) -> CompletableFuture.completedFuture(done()));
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
                                    return done();
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
        if (Status.Starting.equals(currentStatus) || Status.Started.equals(currentStatus)) {
            logger().info("{} already started", name());
            return currentStatus;
        }
        updateStatus(Status.Starting);
        CommitterSettings committerSettings = CommitterSettings.create(actorSystem);

        logger().info("Starting {} {} on topic '{}' with group id '{}'", name(), Integer.toHexString(this.hashCode()), subscription, groupId);

        RestartSource.onFailuresWithBackoff(
                minBackoff,
                maxBackoff,
                randomFactor,
                () -> {
                    this.onStarting.get();
                    logger().info("Stream for {} is starting", name());
                    return Consumer
                            .committablePartitionedSource(consumerSettings, subscription)
                            .flatMapMerge(100, tuple ->
                                    tuple.second()
                                            .via(messageHandling())
                                            .via(Committer.flow(committerSettings.withMaxBatch(commitSize)))
                            )
                            .mapMaterializedValue(control -> {
                                updateStatus(Status.Started);
                                this.onStarted.apply(control);
                                logger().info("Stream for {} has started", name());
                                controlRef.set(control);
                                return control;
                            })
                            .watchTermination((___, done) -> handleTerminaison(done));
                })
                .watchTermination((___, done) -> handleTerminaison(done))
                .runWith(Sink.ignore(), this.materializer);
        return Status.Starting;
    }

    public CompletionStage<Done> stop() {
        updateStatus(Status.Stopping);
        return this.onStopping.apply(controlRef.get())
                .exceptionally(__ -> done())
                .thenCompose(__ -> stopConsumingKafka())
                .exceptionally(__ -> done())
                .whenComplete((__, ___) -> {
                    updateStatus(Status.Stopped);
                })
                .thenCompose(__ -> this.onStopped.get())
                .exceptionally(__ -> done());
    }

    protected CompletionStage<Status> handleTerminaison(CompletionStage<Done> done) {
        return done
                .thenApply(any -> {
                    logger().info("Stopping {}", name());
                    updateStatus(Status.Stopped);
                    return stopConsumingKafka()
                            .exceptionally(__ -> done())
                            .thenCompose(__ -> this.onStopped.get())
                            .<Status>thenApply(__ -> Status.Stopped)
                            .exceptionally(__ -> Status.Stopped);
                })
                .exceptionally(e -> {
                    logger().error("Error during " + name(), e);
                    updateStatus(Status.Failed);
                    return stopConsumingKafka()
                            .exceptionally(__ -> done())
                            .thenCompose(__ -> this.onFailed.apply(e))
                            .<Status>thenApply(__ -> Status.Failed)
                            .exceptionally(__ -> Status.Failed);
                })
                .thenCompose(identity());
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
