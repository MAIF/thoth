package fr.maif.reactor.eventsourcing;

import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventPublisher;
import fr.maif.eventsourcing.EventStore;
import fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.control.Option;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
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
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import static fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy.WAIT;

public class ReactorKafkaEventPublisher<E extends Event, Meta, Context> implements EventPublisher<E, Meta, Context>, Closeable {

    private final static Logger LOGGER = LoggerFactory.getLogger(ReactorKafkaEventPublisher.class);

    private final String topic;
    private final Sinks.Many<EventEnvelope<E, Meta, Context>> queue;
    private final SenderOptions<String, EventEnvelope<E, Meta, Context>> senderOptions;
    private final Flux<EventEnvelope<E, Meta, Context>> eventsSource;
    private final Duration restartInterval;
    private final Duration maxRestartInterval;
    private final Function<Flux<SenderResult<EventEnvelope<E, Meta, Context>>>, Flux<List<SenderResult<EventEnvelope<E, Meta, Context>>>>> groupFlow = it -> it.buffer(1000).map(List::ofAll);
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

        EventEnvelope<E, Meta, Context> e = EventEnvelope.<E, Meta, Context>builder().build();

        this.queue = Sinks.many().multicast().onBackpressureBuffer(queueBufferSize1);
        this.eventsSource = queue.asFlux();
        this.senderOptions = senderOptions;
        this.kafkaSender = KafkaSender.create(senderOptions);
    }

    @Override
    public <TxCtx> void start(EventStore<TxCtx, E, Meta, Context> eventStore, ConcurrentReplayStrategy concurrentReplayStrategy) {
        LOGGER.info("Starting/Restarting publishing event to kafka on topic {}", topic);

        Sinks.Many<EventEnvelope<E, Meta, Context>> logProgressSink = Sinks.many().unicast().onBackpressureBuffer();
        logProgress(logProgressSink.asFlux(), 100).subscribe();
        killSwitch = Mono.fromCompletionStage(eventStore::openTransaction)
                .flatMapMany(tx -> {
                    LOGGER.info("Replaying not published in DB for {}", topic);
                    ConcurrentReplayStrategy strategy = Objects.isNull(concurrentReplayStrategy) ? WAIT : concurrentReplayStrategy;
                    return Flux
                            .from(eventStore.loadEventsUnpublished(tx, strategy))
                            .transform(publishToKafka(eventStore, Option.some(tx), groupFlow))
                            .doOnNext(logProgressSink::tryEmitNext)
                            .concatMap(any -> Mono.fromCompletionStage(() -> {
                                LOGGER.info("Replaying events not published in DB is finished for {}", topic);
                                return eventStore.commitOrRollback(Option.none(), tx);
                            }))
                            .doOnError(e -> {
                                eventStore.commitOrRollback(Option.of(e), tx);
                                LOGGER.error("Error replaying non published events to kafka for " + topic, e);
                            })
                            .collectList()
                            .map(__ -> Tuple.empty());
                })
                .retryWhen(Retry.backoff(10, restartInterval)
                        .transientErrors(true)
                        .maxBackoff(maxRestartInterval)
                        .doBeforeRetry(ctx -> {
                            LOGGER.error("Error republishing events for topic %s retrying for the %s time".formatted(topic, ctx.totalRetries()), ctx.failure());
                        })
                )
                .onErrorReturn(Tuple.empty())
                .switchIfEmpty(Mono.just(Tuple.empty()))
                .concatMap(__ ->
                        this.eventsSource.transform(publishToKafka(
                                eventStore,
                                Option.none(),
                                it -> it
                                        .bufferTimeout(50, Duration.ofMillis(20))
                                        .map(List::ofAll)
                        ))
                )
                .doOnError(e -> LOGGER.error("Error publishing events to kafka", e))
                .doOnComplete(() -> LOGGER.info("Closing publishing to {}", topic))
                .retryWhen(Retry.backoff(Long.MAX_VALUE, restartInterval)
                        .transientErrors(true)
                        .maxBackoff(maxRestartInterval)
                        .doBeforeRetry(ctx -> {
                            LOGGER.error("Error handling events for topic %s retrying for the %s time".formatted(topic, ctx.totalRetries()), ctx.failure());
                        })
                )
                .subscribe();
    }


    private <TxCtx> Function<Flux<EventEnvelope<E, Meta, Context>>, Flux<EventEnvelope<E, Meta, Context>>> publishToKafka(EventStore<TxCtx, E, Meta, Context> eventStore, Option<TxCtx> tx, Function<Flux<SenderResult<EventEnvelope<E, Meta, Context>>>, Flux<List<SenderResult<EventEnvelope<E, Meta, Context>>>>> groupFlow) {
        return it -> it
                .map(this::toKafkaMessage)
                .concatMap(events -> {
                    LOGGER.debug("Sending event {}", events);
                    return kafkaSender.send(Flux.just(events))
                            .doOnError(e -> LOGGER.error("Error publishing to kafka ", e));
                })
                .transform(groupFlow)
                .concatMap(m ->
                        tx.fold(
                                () -> Mono.fromCompletionStage(() -> eventStore.markAsPublished(m.map(SenderResult::correlationMetadata))),
                                txCtx -> Mono.fromCompletionStage(() -> eventStore.markAsPublished(txCtx, m.map(SenderResult::correlationMetadata)))
                        )
                )
                .flatMapIterable(e -> e);
    }

    @Override
    public CompletionStage<Tuple0> publish(List<EventEnvelope<E, Meta, Context>> events) {
        LOGGER.debug("Publishing event in memory : \n{} ", events);
        return Flux
                .fromIterable(events)
                .map(t -> {
                    try {
                        queue.emitNext(t, Sinks.EmitFailureHandler.busyLooping(Duration.ofSeconds(1)));
                        return Tuple.empty();
                    } catch (Exception e) {
                        LOGGER.error("Error publishing to topic %s".formatted(topic), e);
                        return Tuple.empty();
                    }
                })
                .collectList()
                .thenReturn(Tuple.empty())
                .toFuture();
    }

    @Override
    public void close() throws IOException {
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
        return this.queue.scan(Scannable.Attr.BUFFERED);
    }

}
