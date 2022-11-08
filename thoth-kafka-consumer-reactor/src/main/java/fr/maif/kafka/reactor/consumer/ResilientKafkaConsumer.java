package fr.maif.kafka.reactor.consumer;

import io.vavr.Tuple;
import io.vavr.Tuple0;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class ResilientKafkaConsumer<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResilientKafkaConsumer.class);

    public record Config<K, V>(
            Collection<String> topics,
            String groupId,
            ReceiverOptions<K, V> receiverOptions,
            Integer maxRestarts,
            Duration minBackoff,
            Duration maxBackoff,
            Double randomFactor,
            Integer commitSize,
            BiFunction<Disposable, Integer, Mono<Tuple0>> onStarted,
            Supplier<Mono<Tuple0>> onStarting,
            Supplier<Mono<Tuple0>> onStopped,
            Supplier<Mono<Tuple0>> onStopping,
            Function<Throwable, Mono<Tuple0>> onFailed) {


        public static class ConfigBuilder<K, V> {
            Collection<String> topics;
            String groupId;
            ReceiverOptions<K, V> consumerSettings;
            Integer maxRestarts;
            Duration minBackoff;
            Duration maxBackoff;
            Double randomFactor;
            Integer commitSize;
            BiFunction<Disposable, Integer, Mono<Tuple0>> onStarted;
            Supplier<Mono<Tuple0>> onStarting;
            Supplier<Mono<Tuple0>> onStopped;
            Supplier<Mono<Tuple0>> onStopping;
            Function<Throwable, Mono<Tuple0>> onFailed;

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

            public ConfigBuilder<K, V> maxRestarts(Integer maxRestarts) {
                this.maxRestarts = maxRestarts;
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
                    BiFunction<Disposable, Integer, Mono<Tuple0>> onStarted) {
                this.onStarted = onStarted;
                return this;
            }

            public ConfigBuilder<K, V> onStarting(Supplier<Mono<Tuple0>> onStarting) {
                this.onStarting = onStarting;
                return this;
            }

            public ConfigBuilder<K, V> onStopped(Supplier<Mono<Tuple0>> onStopped) {
                this.onStopped = onStopped;
                return this;
            }

            public ConfigBuilder<K, V> onStopping(Supplier<Mono<Tuple0>> onStopping) {
                this.onStopping = onStopping;
                return this;
            }

            public ConfigBuilder<K, V> onFailed(Function<Throwable, Mono<Tuple0>> onFailed) {
                this.onFailed = onFailed;
                return this;
            }

            public Config<K, V> build() {
                return new Config<>(this.topics, this.groupId, this.consumerSettings, this.maxRestarts, this.minBackoff,
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


        public Config<K, V> withMaxRestarts(Integer maxRestarts) {
            return this.toBuilder().maxRestarts(maxRestarts).build();
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
                BiFunction<Disposable, Integer, Mono<Tuple0>> onStarted) {
            return this.toBuilder().onStarted(onStarted).build();
        }

        public Config<K, V> withOnStarting(Supplier<Mono<Tuple0>> onStarting) {
            return this.toBuilder().onStarting(onStarting).build();
        }

        public Config<K, V> withOnStopped(Supplier<Mono<Tuple0>> onStopped) {
            return this.toBuilder().onStopped(onStopped).build();
        }

        public Config<K, V> withOnStopping(
                Supplier<Mono<Tuple0>> onStopping) {
            return this.toBuilder().onStopping(onStopping).build();
        }

        public Config<K, V> withOnFailed(Function<Throwable, Mono<Tuple0>> onFailed) {
            return this.toBuilder().onFailed(onFailed).build();
        }
    }

    protected final Collection<String> topics;
    protected final String groupId;
    protected final Long maxRestarts;
    protected final Duration minBackoff;
    protected final Duration maxBackoff;
    protected final Integer commitSize;
    protected final ReceiverOptions<K, V> receiverOptions;
    protected final BiFunction<Disposable, Integer, Mono<Tuple0>> onStarted;
    protected final Supplier<Mono<Tuple0>> onStarting;
    protected final Supplier<Mono<Tuple0>> onStopped;
    protected final Supplier<Mono<Tuple0>> onStopping;
    protected final Function<Throwable, Mono<Tuple0>> onFailed;

    protected final AtomicReference<Disposable> disposableKafkaRef = new AtomicReference<>();
    protected final AtomicReference<CountDownLatch> cdlRef = new AtomicReference<>();
    protected final AtomicReference<Status> innerStatus = new AtomicReference<>(Status.Stopped);
    protected final AtomicReference<KafkaReceiver<K, V>> consumerRef = new AtomicReference<>();

    public ResilientKafkaConsumer(Config<K, V> config) {
        this.topics = config.topics;
        this.groupId = config.groupId;
        this.maxRestarts = Objects.requireNonNullElse(config.maxRestarts, Long.MAX_VALUE).longValue();
        this.minBackoff = Objects.isNull(config.minBackoff) ? Duration.ofSeconds(30) : config.minBackoff;
        this.maxBackoff = Objects.isNull(config.maxBackoff) ? Duration.ofMinutes(30) : config.maxBackoff;
        this.commitSize = Objects.isNull(config.commitSize) ? 10 : config.commitSize;
        this.receiverOptions = config.receiverOptions
                .commitBatchSize(commitSize)
                .subscription(topics)
                .consumerProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                .consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, config.groupId);

        this.onStarted = defaultIfNull(config.onStarted, (__, ___) -> Mono.just(Tuple.empty()));
        this.onStarting = defaultIfNull(config.onStarting, () -> Mono.just(Tuple.empty()));
        this.onStopped = defaultIfNull(config.onStopped, () -> Mono.just(Tuple.empty()));
        this.onStopping = defaultIfNull(config.onStopping, () -> Mono.just(Tuple.empty()));
        this.onFailed = defaultIfNull(config.onFailed, (__) -> Mono.just(Tuple.empty()));
        this.start();
    }

    public static <K, V> ResilientKafkaConsumer<K, V> createFromFlux(String name,
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
                                                             Function<ReceiverRecord<K, V>, Mono<Tuple0>> handleMessage) {
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
        CountDownLatch cdl = new CountDownLatch(1);
        cdlRef.set(cdl);
        LOGGER.info("Starting {} on topic '{}' with group id '{}'", name(), topics, groupId);
        AtomicInteger restartCount = new AtomicInteger(0);
        KafkaReceiver<K, V> kafkaReceiver = KafkaReceiver.create(this.receiverOptions);
        consumerRef.set(kafkaReceiver);
        var connector = kafkaReceiver.receive()
                .doOnSubscribe(s -> onStart(restartCount))
                .groupBy(m -> m.receiverOffset().topicPartition())
                .flatMap(partition -> partition
                        .transform(messageHandling()).doOnNext(r -> r.receiverOffset().acknowledge())
                )
                .retryWhen(Retry.backoff(maxRestarts, minBackoff)
                        .maxBackoff(maxBackoff)
                        .onRetryExhaustedThrow((spec, rs) -> rs.failure())
                        .doBeforeRetry(s -> {
                            int count = restartCount.incrementAndGet();
                            if (count > 0) {
                                LOGGER.info("Stream for %s is restarting for the %s time".formatted(name(), count), s.failure());
                            } else {
                                LOGGER.info("Stream for {} is starting", name());
                            }
                        })
                )
                .publish();
        connector
                .subscribe(
                        any -> {
                        },
                        e -> {
                            onError(e);
                            cdl.countDown();
                        },
                        () -> {
                            onComplete();
                            cdl.countDown();
                        }
                );
        Disposable disposable = connector.connect();
        disposableKafkaRef.set(disposable);
        return status();
    }

    private void onStart(AtomicInteger restartCount) {
        LOGGER.info("Starting {}", name());
        updateStatus(Status.Started);
        this.onStarted.apply(disposableKafkaRef.get(), restartCount.get()).subscribe();
    }

    private void onError(Throwable err) {
        if (err instanceof CancellationException) {
            onComplete();
        } else {
            LOGGER.error("Error during " + name(), err);
            updateStatus(Status.Failed);
            this.onFailed.apply(err).subscribe();
        }
    }

    private void onComplete() {
        LOGGER.info("{} is stopped", name());
        updateStatus(Status.Stopped);
        this.onStopped.get().subscribe();
    }

    public Mono<Tuple0> stop() {
        return Mono.create(s -> {
            LOGGER.info("Stopping {}", name());
            updateStatus(Status.Stopping);
            this.onStopping.get().subscribe();
            Optional.ofNullable(disposableKafkaRef.get()).ifPresent(disposable -> {
                if (!disposable.isDisposed()) {
                    disposable.dispose();
                }
            });
            Optional.ofNullable(cdlRef.get()).ifPresent(countDownLatch -> {
                try {
                    countDownLatch.await();
                    s.success();
                } catch (InterruptedException e) {
                    s.error(e);
                }
            });
        });
    }
}
