package fr.maif.reactor.eventsourcing;

import fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventPublisher;
import fr.maif.eventsourcing.EventStore;
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
        this.restartInterval = restartInterval == null ? Duration.of(10, ChronoUnit.SECONDS) : restartInterval;
        this.maxRestartInterval = maxRestartInterval == null ? Duration.of(30, ChronoUnit.MINUTES) : maxRestartInterval;

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
                .flux()
                .concatMap(tx -> {
                    LOGGER.info("Replaying not published in DB for {}", topic);
                    ConcurrentReplayStrategy strategy = Objects.isNull(concurrentReplayStrategy) ? WAIT : concurrentReplayStrategy;
                    return Flux.from(eventStore.loadEventsUnpublished(tx, strategy))
                            .transform(publishToKafka(eventStore, Option.some(tx), groupFlow))
                            .doOnNext(logProgressSink::tryEmitNext)
                            .then(Mono.fromCompletionStage(() -> {
                                LOGGER.info("Replaying events not published in DB is finished for {}", topic);
                                return eventStore.commitOrRollback(Option.none(), tx);
                            }))
                            .doOnError(e -> {
                                eventStore.commitOrRollback(Option.of(e), tx);
                                LOGGER.error("Error replaying non published events to kafka for " + topic, e);

                            });
                })
                .thenMany(
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
        Function<Flux<SenderRecord<String, EventEnvelope<E, Meta, Context>, EventEnvelope<E, Meta, Context>>>, Flux<SenderResult<EventEnvelope<E, Meta, Context>>>> publishToKafkaFlow = it ->
                kafkaSender.send(it)
                        .doOnError(e -> {
                            LOGGER.error("Error publishing to kafka ", e);
                        });
        return it -> it
                .map(this::toKafkaMessage)
                .transform(publishToKafkaFlow)
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
                .concatMap(t ->
                    Mono.defer(() -> {
                        Sinks.EmitResult emitResult = queue.tryEmitNext(t);
                        if (emitResult.isFailure()) {
                            return Mono.error(new RuntimeException("Error publishing to queue for %s : %s".formatted(topic, emitResult)));
                        } else {
                            return Mono.just("");
                        }
                    })
                    .retryWhen(Retry
                            .backoff(5, Duration.ofMillis(500))
                            .doBeforeRetry(ctx -> {
                                LOGGER.error("Error publishing to queue %s retrying for the %s time".formatted(topic, ctx.totalRetries()), ctx.failure());
                            })
                    )
                    .onErrorReturn("")
                )
                .collectList()
                .thenReturn(Tuple.empty())
                .toFuture();
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(killSwitch)) {
            this.killSwitch.dispose();
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
