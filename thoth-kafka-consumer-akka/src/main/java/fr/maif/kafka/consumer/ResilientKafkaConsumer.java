package fr.maif.kafka.consumer;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.kafka.AutoSubscription;
import akka.kafka.CommitterSettings;
import akka.kafka.ConsumerMessage;
import akka.kafka.ConsumerSettings;
import akka.kafka.javadsl.Committer;
import akka.kafka.javadsl.Consumer;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.FlowWithContext;
import akka.stream.javadsl.RestartSource;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static akka.Done.done;
import static java.util.function.Function.identity;

public abstract class ResilientKafkaConsumer<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResilientKafkaConsumer.class);
    
    public static class Config<K, V> {
        public final AutoSubscription subscription;
        public final String groupId;
        public final ConsumerSettings<K, V> consumerSettings;
        public Duration minBackoff;
        public Duration maxBackoff;
        public Double randomFactor;
        public Integer commitSize;
        public BiFunction<Consumer.Control, Integer, CompletionStage<Done>> onStarted;
        public Supplier<CompletionStage<Done>> onStarting;
        public Supplier<CompletionStage<Done>> onStopped;
        public Function<Consumer.Control, CompletionStage<Done>> onStopping;
        public Function<Throwable, CompletionStage<Done>> onFailed;

        private Config(AutoSubscription subscription, String groupId, ConsumerSettings<K, V> consumerSettings, Duration minBackoff,
                Duration maxBackoff, Double randomFactor, Integer commitSize,
                BiFunction<Consumer.Control, Integer, CompletionStage<Done>> onStarted,
                Supplier<CompletionStage<Done>> onStarting, Supplier<CompletionStage<Done>> onStopped,
                Function<Consumer.Control, CompletionStage<Done>> onStopping,
                Function<Throwable, CompletionStage<Done>> onFailed) {
            this.subscription = subscription;
            this.groupId = groupId;
            this.consumerSettings = consumerSettings;
            this.minBackoff = minBackoff;
            this.maxBackoff = maxBackoff;
            this.randomFactor = randomFactor;
            this.commitSize = commitSize;
            this.onStarted = onStarted;
            this.onStarting = onStarting;
            this.onStopped = onStopped;
            this.onStopping = onStopping;
            this.onFailed = onFailed;
        }

        public static class ConfigBuilder<K, V> {
            AutoSubscription subscription;
            String groupId;
            ConsumerSettings<K, V> consumerSettings;
            Duration minBackoff;
            Duration maxBackoff;
            Double randomFactor;
            Integer commitSize;
            BiFunction<Consumer.Control, Integer, CompletionStage<Done>> onStarted;
            Supplier<CompletionStage<Done>> onStarting;
            Supplier<CompletionStage<Done>> onStopped;
            Function<Consumer.Control, CompletionStage<Done>> onStopping;
            Function<Throwable, CompletionStage<Done>> onFailed;

            public ConfigBuilder<K, V> subscription(AutoSubscription subscription) {
                this.subscription = subscription;
                return this;
            }

            public ConfigBuilder<K, V> groupId(String groupId) {
                this.groupId = groupId;
                return this;
            }

            public ConfigBuilder<K, V> consumerSettings(ConsumerSettings<K, V> consumerSettings) {
                this.consumerSettings = consumerSettings;
                return this;
            }

            public ConfigBuilder<K,V> minBackoff(Duration minBackoff) {
                this.minBackoff = minBackoff;
                return this;
            }

            public ConfigBuilder<K,V> maxBackoff(Duration maxBackoff) {
                this.maxBackoff = maxBackoff;
                return this;
            }

            public ConfigBuilder<K,V> randomFactor(Double randomFactor) {
                this.randomFactor = randomFactor;
                return this;
            }

            public ConfigBuilder<K,V> commitSize(Integer commitSize) {
                this.commitSize = commitSize;
                return this;
            }

            public ConfigBuilder<K,V> onStarted(
                    BiFunction<Consumer.Control, Integer, CompletionStage<Done>> onStarted) {
                this.onStarted = onStarted;
                return this;
            }

            public ConfigBuilder<K,V> onStarting(Supplier<CompletionStage<Done>> onStarting) {
                this.onStarting = onStarting;
                return this;
            }

            public ConfigBuilder<K,V> onStopped(Supplier<CompletionStage<Done>> onStopped) {
                this.onStopped = onStopped;
                return this;
            }

            public ConfigBuilder<K,V> onStopping(
                    Function<Consumer.Control, CompletionStage<Done>> onStopping) {
                this.onStopping = onStopping;
                return this;
            }

            public ConfigBuilder<K,V> onFailed(Function<Throwable, CompletionStage<Done>> onFailed) {
                this.onFailed = onFailed;
                return this;
            }

            public Config<K,V> build(){
                return new Config<>(this.subscription, this.groupId, this.consumerSettings, this.minBackoff,
                        this.maxBackoff, this.randomFactor, this.commitSize, this.onStarted,
                        this.onStarting, this.onStopped, this.onStopping, this.onFailed);
            }
        }

        public static <K, V> Config<K, V> create(AutoSubscription subscription, String groupId, ConsumerSettings<K, V> consumerSettings) {
            return Config.<K,V>builder()
                    .consumerSettings(consumerSettings)
                    .groupId(groupId)
                    .subscription(subscription)
                    .build();
        }
        
        public static <K,V> ConfigBuilder<K, V> builder(){
            return new ConfigBuilder<>();
        }

        public ConfigBuilder<K, V> toBuilder(){
            return Config.<K,V>builder()
                    .subscription(this.subscription)
                    .groupId(this.groupId)
                    .consumerSettings(this.consumerSettings)
                    .minBackoff(this.minBackoff)
                    .maxBackoff(this.maxBackoff)
                    .randomFactor(this.randomFactor)
                    .commitSize(this.commitSize)
                    .onStarted(this.onStarted)
                    .onStarting(this.onStarting)
                    .onStopped(this.onStopped)
                    .onStopping(this.onStopping)
                    .onFailed(this.onFailed);
        }

        public Config<K,V> withMinBackoff(Duration minBackoff) {
            return this.toBuilder().minBackoff(minBackoff).build();
        }

        public Config<K,V> withMaxBackoff(Duration maxBackoff) {
            return this.toBuilder().maxBackoff(maxBackoff).build();
        }

        public Config<K,V> withRandomFactor(Double randomFactor) {
            return this.toBuilder().randomFactor(randomFactor).build();
        }

        public Config<K,V> withCommitSize(Integer commitSize) {
            return this.toBuilder().commitSize(commitSize).build();
        }

        public Config<K,V> withOnStarted(
                BiFunction<Consumer.Control, Integer, CompletionStage<Done>> onStarted) {
            return this.toBuilder().onStarted(onStarted).build();
        }

        public Config<K,V> withOnStarting(Supplier<CompletionStage<Done>> onStarting) {
            return this.toBuilder().onStarting(onStarting).build();
        }

        public Config<K,V> withOnStopped(Supplier<CompletionStage<Done>> onStopped) {
            return this.toBuilder().onStopped(onStopped).build();
        }

        public Config<K,V> withOnStopping(
                Function<Consumer.Control, CompletionStage<Done>> onStopping) {
            return this.toBuilder().onStopping(onStopping).build();
        }

        public Config<K,V> withOnFailed(Function<Throwable, CompletionStage<Done>> onFailed) {
            return this.toBuilder().onFailed(onFailed).build();
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
    protected final BiFunction<Consumer.Control, Integer, CompletionStage<Done>> onStarted;
    protected final Supplier<CompletionStage<Done>> onStarting;
    protected final Supplier<CompletionStage<Done>> onStopped;
    protected final Function<Consumer.Control, CompletionStage<Done>> onStopping;
    protected final Function<Throwable, CompletionStage<Done>> onFailed;

    protected final AtomicReference<Consumer.Control> controlRef = new AtomicReference<>();
    protected final AtomicReference<Status> innerStatus = new AtomicReference<>(Status.Stopped);

    public ResilientKafkaConsumer(ActorSystem actorSystem, Config<K, V> config) {
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
        this.onStarted = defaultIfNull(config.onStarted, (__, ___) -> CompletableFuture.completedFuture(done()));
        this.onStarting = defaultIfNull(config.onStarting, () -> CompletableFuture.completedFuture(done()));
        this.onStopped = defaultIfNull(config.onStopped, () -> CompletableFuture.completedFuture(done()));
        this.onStopping = defaultIfNull(config.onStopping, (__) -> CompletableFuture.completedFuture(done()));
        this.onFailed = defaultIfNull(config.onFailed, (__) -> CompletableFuture.completedFuture(done()));
        this.start();
    }

    public static <K, V> ResilientKafkaConsumer<K, V> createFromFlow(ActorSystem actorSystem,
                                                             String name,
                                                             Config<K, V> config,
                                                             Flow<ConsumerMessage.CommittableMessage<K, V>, ConsumerMessage.CommittableOffset, NotUsed> messageHandling) {
        return new ResilientKafkaConsumer<K, V>(actorSystem, config) {
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

    public static <K, V> ResilientKafkaConsumer<K, V> createFromFlowCtx(ActorSystem actorSystem,
                                                             String name,
                                                             Config<K, V> config,
                                                             FlowWithContext<ConsumerMessage.CommittableMessage<K, V>, ConsumerMessage.CommittableOffset, ?, ConsumerMessage.CommittableOffset, NotUsed> messageHandling) {
        return new ResilientKafkaConsumer<K, V>(actorSystem, config) {
            @Override
            protected String name() {
                return name;
            }

            @Override
            public Flow<ConsumerMessage.CommittableMessage<K, V>, ConsumerMessage.CommittableOffset, NotUsed> messageHandling() {
                return Flow.<ConsumerMessage.CommittableMessage<K, V>>create()
                        .map(m -> Pair.create(m, m.committableOffset()))
                        .via(messageHandling.asFlow())
                        .map(Pair::second);
            }
        };
    }

    public static <K, V> ResilientKafkaConsumer<K, V> createFromFlowCtxAgg(ActorSystem actorSystem,
                                                             String name,
                                                             Config<K, V> config,
                                                             FlowWithContext<ConsumerMessage.CommittableMessage<K, V>, ConsumerMessage.CommittableOffset, ?, List<ConsumerMessage.CommittableOffset>, NotUsed> messageHandling) {
        return new ResilientKafkaConsumer<K, V>(actorSystem, config) {
            @Override
            protected String name() {
                return name;
            }

            @Override
            public Flow<ConsumerMessage.CommittableMessage<K, V>, ConsumerMessage.CommittableOffset, NotUsed> messageHandling() {
                return Flow.<ConsumerMessage.CommittableMessage<K, V>>create()
                        .map(m -> Pair.create(m, m.committableOffset()))
                        .via(messageHandling.asFlow())
                        .mapConcat(Pair::second);
            }
        };
    }

    public static <K, V> ResilientKafkaConsumer<K, V> create(ActorSystem actorSystem,
                                                             String name,
                                                             Config<K, V> config,
                                                             Function<ConsumerMessage.CommittableMessage<K, V>, CompletionStage<Done>> handleMessage) {
        return new ResilientKafkaConsumer<K, V>(actorSystem, config) {
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

    public static <K, V> ResilientKafkaConsumer<K, V> create(ActorSystem actorSystem,
                                                             String name,
                                                             Config<K, V> config,
                                                             Executor executor,
                                                             java.util.function.Consumer<ConsumerMessage.CommittableMessage<K, V>> handleMessage) {
        return new ResilientKafkaConsumer<K, V>(actorSystem, config) {
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

    public static <K, V> ResilientKafkaConsumer<K, V> create(ActorSystem actorSystem,
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
            LOGGER.info("{} already started", name());
            return currentStatus;
        }
        updateStatus(Status.Starting);
        CommitterSettings committerSettings = CommitterSettings.create(actorSystem);

        LOGGER.info("Starting {} on topic '{}' with group id '{}'", name(), subscription, groupId);
        AtomicInteger restartCount = new AtomicInteger(0);
        RestartSource.onFailuresWithBackoff(
                minBackoff,
                maxBackoff,
                randomFactor,
                () -> {
                    this.onStarting.get();
                    int count = restartCount.incrementAndGet();
                    if (count > 1) {
                        LOGGER.info("Stream for {} is restarting for the {} time", name(), count);
                    } else {
                        LOGGER.info("Stream for {} is starting", name());
                    }
                    return Consumer
                            .committablePartitionedSource(consumerSettings, subscription)
                            .flatMapMerge(100, tuple ->
                                    tuple.second()
                                            .via(messageHandling())
                                            .via(Committer.flow(committerSettings.withMaxBatch(commitSize)))
                            )
                            .mapMaterializedValue(control -> {
                                updateStatus(Status.Started);
                                this.onStarted.apply(control, count);
                                LOGGER.info("Stream for {} has started", name());
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
                    LOGGER.info("Stopping {}", name());
                    updateStatus(Status.Stopped);
                    return stopConsumingKafka()
                            .exceptionally(__ -> done())
                            .thenCompose(__ -> this.onStopped.get())
                            .<Status>thenApply(__ -> Status.Stopped)
                            .exceptionally(__ -> Status.Stopped);
                })
                .exceptionally(e -> {
                    LOGGER.error("Error during " + name(), e);
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
                            LOGGER.error("Error shutting down kafka consumer for {}", name());
                        } else {
                            LOGGER.info("Kafka consumer for {} is shutdown", name());
                        }
                    })
                    .thenCompose(___ -> control.isShutdown())
                    .thenApply(__ -> done());
        } else {
            return CompletableFuture.completedFuture(done());
        }
    }
}
