package fr.maif.reactor.eventsourcing;

import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventPublisher;
import fr.maif.eventsourcing.EventStore;
import fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.collection.Traversable;
import io.vavr.control.Option;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.util.retry.Retry;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy.WAIT;

public class ReactorKafkaEventPublisher<E extends Event, Meta, Context> implements EventPublisher<E, Meta, Context>, Closeable {

    private final static Logger LOGGER = LoggerFactory.getLogger(ReactorKafkaEventPublisher.class);

    private AtomicBoolean stop = new AtomicBoolean(false);
    private final String topic;

    private final ReactorQueue<EventEnvelope<E, Meta, Context>> reactorQueue;
    private final Sinks.Many<EventEnvelope<E, Meta, Context>> queue;
    private final Flux<EventEnvelope<E, Meta, Context>> eventSource;
    private final SenderOptions<String, EventEnvelope<E, Meta, Context>> senderOptions;
    private final Duration restartInterval;
    private final Duration maxRestartInterval;
    private Disposable killSwitch;
    private KafkaSender<String, EventEnvelope<E, Meta, Context>> kafkaSender;

    public ReactorKafkaEventPublisher(SenderOptions<String, EventEnvelope<E, Meta, Context>> senderOptions, String topic) {
        this(senderOptions, topic, null);
    }

    public ReactorKafkaEventPublisher(SenderOptions<String, EventEnvelope<E, Meta, Context>> senderOptions, String topic, Integer queueBufferSize) {
        this(senderOptions, topic, queueBufferSize, Duration.of(10, ChronoUnit.SECONDS), Duration.of(30, ChronoUnit.MINUTES));
    }

    public ReactorKafkaEventPublisher(SenderOptions<String, EventEnvelope<E, Meta, Context>> senderOptions, String topic, Integer queueBufferSize, Duration restartInterval, Duration maxRestartInterval) {
        this.topic = topic;
        int queueBufferSize1 = queueBufferSize == null ? 10000 : queueBufferSize;
        this.restartInterval = restartInterval == null ? Duration.of(1, ChronoUnit.SECONDS) : restartInterval;
        this.maxRestartInterval = maxRestartInterval == null ? Duration.of(1, ChronoUnit.MINUTES) : maxRestartInterval;

        this.reactorQueue = new ReactorQueue<>(queueBufferSize1);
        this.queue = Sinks.many().replay().limit(queueBufferSize1); // .multicast().onBackpressureBuffer(queueBufferSize1);
        this.eventSource = queue.asFlux();
        this.senderOptions = senderOptions;
        this.kafkaSender = KafkaSender.create(senderOptions);
    }

    record CountAndMaxSeqNum(Long count, Long lastSeqNum) {
        static CountAndMaxSeqNum empty() {
            return new CountAndMaxSeqNum(0L, 0L);
        }

        CountAndMaxSeqNum handleSeqNum(Long lastSeqNum) {
            return new CountAndMaxSeqNum(count + 1, Math.max(this.lastSeqNum, lastSeqNum));
        }
    }


    private <T> Function<Flux<T>, Flux<List<T>>> fixedSizeGroup(int size) {
        return it -> it.buffer(size).map(List::ofAll);
    }

    private <T> Function<Flux<T>, Flux<List<T>>> bufferTimeout(int size, Duration duration) {
        return it -> it.bufferTimeout(size, duration, true).map(List::ofAll);
    }

    @Override
    public <TxCtx> void start(EventStore<TxCtx, E, Meta, Context> eventStore, ConcurrentReplayStrategy concurrentReplayStrategy) {
        LOGGER.info("Starting event publisher for topic {}", topic);

        Sinks.Many<EventEnvelope<E, Meta, Context>> logProgressSink = Sinks.many().unicast().onBackpressureBuffer();
        logProgress(logProgressSink.asFlux(), 100).subscribe();
        killSwitch = Mono.defer(() -> fromCS(eventStore::openTransaction)
                        .flatMap(tx -> {
                            LOGGER.info("Replaying events not published from DB in topic {}", topic);
                            ConcurrentReplayStrategy strategy = Objects.isNull(concurrentReplayStrategy) ? WAIT : concurrentReplayStrategy;
                            return Flux
                                    .from(eventStore.loadEventsUnpublished(tx, strategy))
                                    .transform(publishToKafka(eventStore, Option.some(tx), fixedSizeGroup(1000), fixedSizeGroup(1000)))
                                    .doOnNext(logProgressSink::tryEmitNext)
                                    .reduce(CountAndMaxSeqNum.empty(), (c, elt) -> c.handleSeqNum(elt.sequenceNum))
                                    .flatMap(count -> {
                                        LOGGER.info("Replaying events not published in DB is finished for {}, {} elements published", topic, count.count);
                                        return fromCS(() -> eventStore.commitOrRollback(Option.none(), tx))
                                                .thenReturn(count);
                                    })
                                    .doOnError(e -> {
                                        eventStore.commitOrRollback(Option.of(e), tx);
                                        LOGGER.error("Error replaying non published events to kafka for " + topic, e);
                                    })
                                    .flatMap(c -> {
                                        if (c.count == 0) {
                                            return fromCS(()-> eventStore.lastPublishedSequence()).map(l -> new CountAndMaxSeqNum(0L, l));
                                        } else {
                                            return Mono.just(c);
                                        }
                                    });
                        }))
                .flux()
                .concatMap(countAndLastSeqNum -> {
//                        Flux.defer(() -> {
                            LOGGER.debug("Starting consuming in memory queue for {}. Event lower than {} are ignored", topic, countAndLastSeqNum.lastSeqNum);
                            //return reactorQueue.asFlux()
                            return eventSource
                                    .filter(e -> e.sequenceNum > countAndLastSeqNum.lastSeqNum)
                                    .transform(publishToKafka(
                                            eventStore,
                                            Option.none(),
                                            bufferTimeout(200, Duration.ofMillis(20)),
                                            bufferTimeout(200, Duration.ofMillis(20))
                                    ));
                })
                .doOnError(e -> LOGGER.error("Error publishing events to kafka", e))
                .retryWhen(Retry.backoff(Long.MAX_VALUE, restartInterval)
                        .transientErrors(true)
                        .maxBackoff(maxRestartInterval)
                        .doBeforeRetry(ctx -> {
                            LOGGER.error("Error handling events for topic %s retrying for the %s time".formatted(topic, ctx.totalRetries() + 1), ctx.failure());
                        })
                )
                .subscribe();
    }


    public static class ReactorQueue<E> {
        private final Queue<E> innerQueue;
        private final AtomicReference<Runnable> lastOnPush = new AtomicReference<>();
        private final AtomicReference<FluxSink<E>> lastSubscriber = new AtomicReference<>();

        public ReactorQueue(int capacity) {
            this.innerQueue = new ArrayBlockingQueue<>(capacity);
        }

        public void offer(List<E> list) {
            innerQueue.addAll(list.toJavaList());
            Runnable runnable = lastOnPush.get();
            if (runnable != null) {
                runnable.run();
            }
        }

        public Flux<E> asFlux() {
            return Flux.create(sink -> {
                FluxSink<E> lastSink = lastSubscriber.get();
                if (lastSink != null) {
                    lastSink.complete();
                }
                lastSubscriber.set(sink);
                AtomicLong request = new AtomicLong();
                Runnable publishToQueue = () -> {
                    while (!innerQueue.isEmpty() && request.get() > 0) {
                        sink.next(innerQueue.remove());
                    }
                };
                lastOnPush.set(publishToQueue);
                sink.onRequest(count -> {
                    request.getAndAccumulate(count, Long::sum);
                    publishToQueue.run();
                });
            });
        }
    }


    private <TxCtx> Function<Flux<EventEnvelope<E, Meta, Context>>, Flux<EventEnvelope<E, Meta, Context>>> publishToKafka(EventStore<TxCtx, E, Meta, Context> eventStore,
                                                                                                                          Option<TxCtx> tx,
                                                                                                                          Function<Flux<SenderRecord<String, EventEnvelope<E, Meta, Context>, EventEnvelope<E, Meta, Context>>>, Flux<List<SenderRecord<String, EventEnvelope<E, Meta, Context>, EventEnvelope<E, Meta, Context>>>>> groupFlowForKafka,
                                                                                                                          Function<Flux<SenderResult<EventEnvelope<E, Meta, Context>>>, Flux<List<SenderResult<EventEnvelope<E, Meta, Context>>>>> groupFlow
    ) {
        return it -> it
                .map(this::toKafkaMessage)
                .transform(groupFlowForKafka)
                .filter(Traversable::nonEmpty)
                .concatMap(events -> {
                    LOGGER.debug("Sending event {}", events);
                    return kafkaSender.send(Flux.fromIterable(events))
                            .doOnError(e -> LOGGER.error("Error publishing to kafka ", e));
                })
                .transform(groupFlow)
                .filter(Traversable::nonEmpty)
                .concatMap(m ->
                        tx.fold(
                                () -> fromCS(() -> eventStore.markAsPublished(m.map(SenderResult::correlationMetadata))),
                                txCtx -> fromCS(() -> eventStore.markAsPublished(txCtx, m.map(SenderResult::correlationMetadata)))
                        )
                )
                .flatMapIterable(e -> e);
    }

    static <T> Mono<T> fromCS(Supplier<CompletionStage<T>> cs) {
        return Mono.fromFuture(() -> cs.get().toCompletableFuture());
//        return Mono.create(s ->
//                cs.get().whenComplete((r, e) -> {
//                    if (e != null) {
//                        s.error(e);
//                    } else {
//                        s.success(r);
//                    }
//                })
//        );
    }

    @Override
    public CompletionStage<Tuple0> publish(List<EventEnvelope<E, Meta, Context>> events) {
//        LOGGER.debug("Publishing event in memory : \n{} ", events);
//        return Mono.fromCallable(() -> {
//            reactorQueue.offer(events);
//            return Tuple.empty();
//        }).publishOn(Schedulers.boundedElastic()).toFuture();
        return Flux
                .fromIterable(events)
                .map(t -> {
                        queue.tryEmitNext(t).orThrow();
                        return Tuple.empty();
                })
                .retryWhen(Retry.fixedDelay(50, Duration.ofMillis(2))
                        .transientErrors(true)
                        .doBeforeRetry(ctx -> {
                            LOGGER.error("Error publishing events in memory queue for topic %s retrying for the %s time".formatted(topic, ctx.totalRetries() + 1), ctx.failure());
                        })
                )
                .onErrorResume(e -> Mono.just(Tuple.empty()))
                .collectList()
                .thenReturn(Tuple.empty())
                .toFuture();
    }

    @Override
    public void close() throws IOException {
        stop.set(true);
        if (Objects.nonNull(killSwitch) && !killSwitch.isDisposed()) {
            try {
                this.killSwitch.dispose();
            } catch (UnsupportedOperationException e) {
                LOGGER.error("Error closing Publisher", e);
            }
        }
        this.kafkaSender.close();
    }


    private SenderRecord<String, EventEnvelope<E, Meta, Context>, EventEnvelope<E, Meta, Context>> toKafkaMessage(EventEnvelope<E, Meta, Context> eventEnvelope) {
        LOGGER.debug("Publishing to kafka topic {} : \n{}", topic, eventEnvelope);
        return SenderRecord.create(
                new ProducerRecord<>(
                        topic,
                        eventEnvelope.event.hash(),
                        eventEnvelope
                ),
                eventEnvelope
        );
    }


    private <Any> Flux<Integer> logProgress(Flux<Any> logProgress, int every) {
        return logProgress
                .scan(0, (acc, elt) -> acc + 1)
                .doOnNext(count -> {
                    if (count % every == 0) {
                        LOGGER.info("Replayed {} events on {}", count, topic);
                    }
                });
    }

    public Integer getBufferedElementCount() {
//        return this.queue.scan(Scannable.Attr.BUFFERED);
        return 0;
    }

}
