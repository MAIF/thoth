package fr.maif.kafka.reactor.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class ResilientKafkaConsumer<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResilientKafkaConsumer.class);

    public static class Config<K, V> {
        public final Collection<String> topics;
        public final String groupId;
        public final ReceiverOptions<K, V> receiverOptions;
        public Duration minBackoff;
        public Duration maxBackoff;
        public Double randomFactor;
        public Integer commitSize;
        public BiFunction<Disposable, Integer, Mono<Void>> onStarted;
        public Supplier<Mono<Void>> onStarting;
        public Supplier<Mono<Void>> onStopped;
        public Function<Disposable, Mono<Void>> onStopping;
        public Function<Throwable, Mono<Void>> onFailed;

        private Config(Collection<String> topics, String groupId, ReceiverOptions<K, V> receiverOptions, Duration minBackoff,
                       Duration maxBackoff, Double randomFactor, Integer commitSize,
                       BiFunction<Disposable, Integer, Mono<Void>> onStarted,
                       Supplier<Mono<Void>> onStarting, Supplier<Mono<Void>> onStopped,
                       Function<Disposable, Mono<Void>> onStopping,
                       Function<Throwable, Mono<Void>> onFailed) {
            this.topics = topics;
            this.groupId = groupId;
            this.receiverOptions = receiverOptions;
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
            Collection<String> topics;
            String groupId;
            ReceiverOptions<K, V> consumerSettings;
            Duration minBackoff;
            Duration maxBackoff;
            Double randomFactor;
            Integer commitSize;
            BiFunction<Disposable, Integer, Mono<Void>> onStarted;
            Supplier<Mono<Void>> onStarting;
            Supplier<Mono<Void>> onStopped;
            Function<Disposable, Mono<Void>> onStopping;
            Function<Throwable, Mono<Void>> onFailed;

            public ConfigBuilder<K, V> topics(Collection<String> topics) {
                this.topics = topics;
                return this;
            }

            public ConfigBuilder<K, V> groupId(String groupId) {
                this.groupId = groupId;
                return this;
            }

            public ConfigBuilder<K, V> receiverOptions(ReceiverOptions<K, V> consumerSettings) {
                this.consumerSettings = consumerSettings;
                return this;
            }

            public ConfigBuilder<K, V> minBackoff(Duration minBackoff) {
                this.minBackoff = minBackoff;
                return this;
            }

            public ConfigBuilder<K, V> maxBackoff(Duration maxBackoff) {
                this.maxBackoff = maxBackoff;
                return this;
            }

            public ConfigBuilder<K, V> randomFactor(Double randomFactor) {
                this.randomFactor = randomFactor;
                return this;
            }

            public ConfigBuilder<K, V> commitSize(Integer commitSize) {
                this.commitSize = commitSize;
                return this;
            }

            public ConfigBuilder<K, V> onStarted(
                    BiFunction<Disposable, Integer, Mono<Void>> onStarted) {
                this.onStarted = onStarted;
                return this;
            }

            public ConfigBuilder<K, V> onStarting(Supplier<Mono<Void>> onStarting) {
                this.onStarting = onStarting;
                return this;
            }

            public ConfigBuilder<K, V> onStopped(Supplier<Mono<Void>> onStopped) {
                this.onStopped = onStopped;
                return this;
            }

            public ConfigBuilder<K, V> onStopping(
                    Function<Disposable, Mono<Void>> onStopping) {
                this.onStopping = onStopping;
                return this;
            }

            public ConfigBuilder<K, V> onFailed(Function<Throwable, Mono<Void>> onFailed) {
                this.onFailed = onFailed;
                return this;
            }

            public Config<K, V> build() {
                return new Config<>(this.topics, this.groupId, this.consumerSettings, this.minBackoff,
                        this.maxBackoff, this.randomFactor, this.commitSize, this.onStarted,
                        this.onStarting, this.onStopped, this.onStopping, this.onFailed);
            }
        }

        public static <K, V> Config<K, V> create(Collection<String> topics, String groupId, ReceiverOptions<K, V> consumerSettings) {
            return Config.<K, V>builder()
                    .receiverOptions(consumerSettings)
                    .groupId(groupId)
                    .topics(topics)
                    .build();
        }

        public static <K, V> ConfigBuilder<K, V> builder() {
            return new ConfigBuilder<>();
        }

        public ConfigBuilder<K, V> toBuilder() {
            return Config.<K, V>builder()
                    .topics(this.topics)
                    .groupId(this.groupId)
                    .receiverOptions(this.receiverOptions)
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

        public Config<K, V> withMinBackoff(Duration minBackoff) {
            return this.toBuilder().minBackoff(minBackoff).build();
        }

        public Config<K, V> withMaxBackoff(Duration maxBackoff) {
            return this.toBuilder().maxBackoff(maxBackoff).build();
        }

        public Config<K, V> withRandomFactor(Double randomFactor) {
            return this.toBuilder().randomFactor(randomFactor).build();
        }

        public Config<K, V> withCommitSize(Integer commitSize) {
            return this.toBuilder().commitSize(commitSize).build();
        }

        public Config<K, V> withOnStarted(
                BiFunction<Disposable, Integer, Mono<Void>> onStarted) {
            return this.toBuilder().onStarted(onStarted).build();
        }

        public Config<K, V> withOnStarting(Supplier<Mono<Void>> onStarting) {
            return this.toBuilder().onStarting(onStarting).build();
        }

        public Config<K, V> withOnStopped(Supplier<Mono<Void>> onStopped) {
            return this.toBuilder().onStopped(onStopped).build();
        }

        public Config<K, V> withOnStopping(
                Function<Disposable, Mono<Void>> onStopping) {
            return this.toBuilder().onStopping(onStopping).build();
        }

        public Config<K, V> withOnFailed(Function<Throwable, Mono<Void>> onFailed) {
            return this.toBuilder().onFailed(onFailed).build();
        }
    }

    protected final Collection<String> topics;
    protected final String groupId;
    protected final Duration minBackoff;
    protected final Duration maxBackoff;
    protected final Integer commitSize;
    protected final ReceiverOptions<K, V> receiverOptions;
    protected final BiFunction<Disposable, Integer, Mono<Void>> onStarted;
    protected final Supplier<Mono<Void>> onStarting;
    protected final Supplier<Mono<Void>> onStopped;
    protected final Function<Disposable, Mono<Void>> onStopping;
    protected final Function<Throwable, Mono<Void>> onFailed;

    protected final AtomicReference<Disposable> controlRef = new AtomicReference<>();
    protected final AtomicReference<Status> innerStatus = new AtomicReference<>(Status.Stopped);

    public ResilientKafkaConsumer(Config<K, V> config) {
        this.topics = config.topics;
        this.groupId = config.groupId;
        this.minBackoff = Objects.isNull(config.minBackoff) ? Duration.ofSeconds(30) : config.minBackoff;
        this.maxBackoff = Objects.isNull(config.maxBackoff) ? Duration.ofMinutes(30) : config.maxBackoff;
        this.commitSize = Objects.isNull(config.commitSize) ? 10 : config.commitSize;
        this.receiverOptions = config.receiverOptions
                .commitBatchSize(commitSize)
                .subscription(topics)
                .consumerProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                .consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, config.groupId);

        this.onStarted = defaultIfNull(config.onStarted, (__, ___) -> Mono.just("").then());
        this.onStarting = defaultIfNull(config.onStarting, () -> Mono.just("").then());
        this.onStopped = defaultIfNull(config.onStopped, () -> Mono.just("").then());
        this.onStopping = defaultIfNull(config.onStopping, (__) -> Mono.just("").then());
        this.onFailed = defaultIfNull(config.onFailed, (__) -> Mono.just("").then());
        this.start();
    }

    public static <K, V> ResilientKafkaConsumer<K, V> createFromFlow(String name,
                                                                     Config<K, V> config,
                                                                     Function<Flux<ReceiverRecord<K, V>>, Flux<ReceiverRecord<K, V>>> messageHandling) {
        return new ResilientKafkaConsumer<K, V>(config) {
            @Override
            protected String name() {
                return name;
            }

            @Override
            public Function<Flux<ReceiverRecord<K, V>>, Flux<ReceiverRecord<K, V>>> messageHandling() {
                return messageHandling;
            }
        };
    }

    public static <K, V> ResilientKafkaConsumer<K, V> create(String name,
                                                             Config<K, V> config,
                                                             Function<ReceiverRecord<K, V>, Mono<Void>> handleMessage) {
        return new ResilientKafkaConsumer<K, V>(config) {
            @Override
            protected String name() {
                return name;
            }

            @Override
            public Function<Flux<ReceiverRecord<K, V>>, Flux<ReceiverRecord<K, V>>> messageHandling() {
                return it -> it
                        .concatMap(m ->
                                handleMessage.apply(m)
                                        .map(__ -> m)
                        );
            }
        };
    }

    public static <K, V> ResilientKafkaConsumer<K, V> create(String name,
                                                             Config<K, V> config,
                                                             java.util.function.Consumer<ReceiverRecord<K, V>> handleMessage) {
        return new ResilientKafkaConsumer<K, V>(config) {
            @Override
            protected String name() {
                return name;
            }

            @Override
            public Function<Flux<ReceiverRecord<K, V>>, Flux<ReceiverRecord<K, V>>> messageHandling() {
                return it -> it
                        .concatMap(m ->
                                Mono.fromCallable(() -> {
                                            handleMessage.accept(m);
                                            return "";
                                        })
                                        .publishOn(Schedulers.parallel())
                                        .map(__ -> m)
                        );
            }
        };
    }

    protected static <T> T defaultIfNull(T v, T defaultValue) {
        if (Objects.isNull(v)) {
            return defaultValue;
        } else {
            return v;
        }
    }

    protected abstract String name();

    protected abstract Function<Flux<ReceiverRecord<K, V>>, Flux<ReceiverRecord<K, V>>> messageHandling();

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
        this.onStarting.get().subscribe();

        LOGGER.info("Starting {} on topic '{}' with group id '{}'", name(), topics, groupId);
        AtomicInteger restartCount = new AtomicInteger(0);
        KafkaReceiver<K, V> kafkaReceiver = KafkaReceiver.create(this.receiverOptions);
        Disposable disposable = kafkaReceiver.receive()
                .doOnSubscribe(s -> {
                    updateStatus(Status.Started);
                    this.onStarted.apply(controlRef.get(), restartCount.get()).subscribe();
                })
                .groupBy(m -> m.receiverOffset().topicPartition())
                .flatMap(
                        partition ->
                                partition.transform(messageHandling())
                                        .doOnNext(r -> r.receiverOffset().acknowledge())
                )
                .retryWhen(Retry.backoff(10, minBackoff)
                        .maxBackoff(maxBackoff)
                        .maxAttempts(Long.MAX_VALUE)
                        .doBeforeRetry(s -> {
                            int count = restartCount.incrementAndGet();
                            if (count > 1) {
                                LOGGER.info("Stream for {} is restarting for the {} time", name(), count);
                            } else {
                                LOGGER.info("Stream for {} is starting", name());
                            }
                        })
                )
                .doOnComplete(() -> {
                    LOGGER.info("Stopping {}", name());
                    updateStatus(Status.Stopped);
                    this.onStopped.get().subscribe();
                })
                .doOnError(err -> {
                    LOGGER.error("Error during " + name(), err);
                    updateStatus(Status.Failed);
                    this.onFailed.apply(err).subscribe();
                })
                .subscribe();
        controlRef.set(disposable);
        return status();
    }

    public Mono<Void> stop() {
        return Mono.fromRunnable(() -> {
            updateStatus(Status.Stopping);
            Optional.ofNullable(controlRef.get()).ifPresent(Disposable::dispose);
            this.onStopping.apply(controlRef.get()).subscribe();
        });
    }
}
