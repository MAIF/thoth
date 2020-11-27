package fr.maif.eventsourcing.impl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.kafka.ProducerMessage;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.Producer;
import akka.stream.KillSwitches;
import akka.stream.Materializer;
import akka.stream.OverflowStrategy;
import akka.stream.RestartSettings;
import akka.stream.UniqueKillSwitch;
import akka.stream.javadsl.*;
import fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventPublisher;
import fr.maif.eventsourcing.EventStore;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;
import io.vavr.control.Option;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy.WAIT;

public class KafkaEventPublisher<E extends Event, Meta, Context> implements EventPublisher<E, Meta, Context>, Closeable {

    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaEventPublisher.class);

    private final Materializer materializer;
    private final String topic;
    private final org.apache.kafka.clients.producer.Producer<String, EventEnvelope<E, Meta, Context>> kafkaProducer;
    private final SourceQueueWithComplete<EventEnvelope<E, Meta, Context>> queue;
    private final ProducerSettings<String, EventEnvelope<E, Meta, Context>> producerSettings;
    private final Source<EventEnvelope<E, Meta, Context>, NotUsed> eventsSource;
    private final Duration restartInterval;
    private final Duration maxRestartInterval;
    private final Flow<ProducerMessage.Results<String, EventEnvelope<E, Meta, Context>, EventEnvelope<E, Meta, Context>>, List<ProducerMessage.Results<String, EventEnvelope<E, Meta, Context>, EventEnvelope<E, Meta, Context>>>, NotUsed> groupFlow = Flow.<ProducerMessage.Results<String, EventEnvelope<E, Meta, Context>, EventEnvelope<E, Meta, Context>>>create().grouped(1000).map(List::ofAll);
    private UniqueKillSwitch killSwitch;

    public KafkaEventPublisher(ActorSystem system, ProducerSettings<String, EventEnvelope<E, Meta, Context>> producerSettings, String topic) {
        this(system, producerSettings, topic, null);
    }

    public KafkaEventPublisher(ActorSystem system, ProducerSettings<String, EventEnvelope<E, Meta, Context>> producerSettings, String topic, Integer queueBufferSize) {
        this(system, producerSettings, topic, queueBufferSize, Duration.of(10, ChronoUnit.SECONDS), Duration.of(30, ChronoUnit.MINUTES));
    }

    public KafkaEventPublisher(ActorSystem system, ProducerSettings<String, EventEnvelope<E, Meta, Context>> producerSettings, String topic, Integer queueBufferSize, Duration restartInterval, Duration maxRestartInterval) {
        this.materializer = Materializer.createMaterializer(system);
        this.topic = topic;
        int queueBufferSize1 = queueBufferSize == null ? 10000 : queueBufferSize;
        this.restartInterval = restartInterval == null ? Duration.of(10, ChronoUnit.SECONDS) : restartInterval;
        this.maxRestartInterval = maxRestartInterval == null ? Duration.of(30, ChronoUnit.MINUTES) : maxRestartInterval;

        EventEnvelope<E, Meta, Context> e = EventEnvelope.<E, Meta, Context>builder().build();

        Source<EventEnvelope<E, Meta, Context>, SourceQueueWithComplete<EventEnvelope<E, Meta, Context>>> queue = Source.queue(queueBufferSize1, OverflowStrategy.backpressure());

        RunnableGraph<Pair<SourceQueueWithComplete<EventEnvelope<E, Meta, Context>>, Source<EventEnvelope<E, Meta, Context>, NotUsed>>> tmpVar = queue
                .toMat(BroadcastHub.of((Class<EventEnvelope<E, Meta, Context>>) e.getClass(), 256), Keep.both());

        Pair<SourceQueueWithComplete<EventEnvelope<E, Meta, Context>>, Source<EventEnvelope<E, Meta, Context>, NotUsed>> pair = tmpVar
                .run(materializer);
        this.kafkaProducer = producerSettings.createKafkaProducer();
        this.producerSettings = producerSettings.withProducer(this.kafkaProducer);
        this.queue = pair.first();
        this.eventsSource = pair.second();
    }

    public <TxCtx> void start(EventStore<TxCtx, E, Meta, Context> eventStore, ConcurrentReplayStrategy concurrentReplayStrategy) {
        killSwitch = RestartSource
                .onFailuresWithBackoff(
                        restartInterval,
                        maxRestartInterval,
                        0,
                        () -> {
                            LOGGER.info("Starting/Restarting publishing event to kafka on topic {}", topic);
                            return Source.completionStage(eventStore.openTransaction().toCompletableFuture())
                                    .flatMapConcat(tx -> {

                                        LOGGER.info("Replaying not published in DB for {}", topic);
                                        ConcurrentReplayStrategy strategy = Objects.isNull(concurrentReplayStrategy) ? WAIT : concurrentReplayStrategy;
                                        return eventStore
                                                .loadEventsUnpublished(tx, strategy)
                                                .via(publishToKafka(eventStore, Option.some(tx), groupFlow))
                                                .alsoTo(logProgress(100))
                                                .watchTermination((nu, cs) ->
                                                        cs.whenComplete((d, e) -> {
                                                            eventStore.commitOrRollback(Option.of(e), tx);
                                                            if (e != null) {
                                                                LOGGER.error("Error replaying non published events to kafka for "+topic, e);
                                                            } else {
                                                                LOGGER.info("Replaying events not published in DB is finished for {}", topic);
                                                            }
                                                        })
                                                );
                                    })
                                    .concat(
                                            this.eventsSource.via(publishToKafka(
                                                    eventStore,
                                                    Option.none(),
                                                    Flow.<ProducerMessage.Results<String, EventEnvelope<E, Meta, Context>, EventEnvelope<E, Meta, Context>>>create()
                                                            .groupedWithin(50, Duration.ofMillis(20))
                                                            .map(List::ofAll)
                                            ))
                                    )
                                    .watchTermination((notUsed, done) -> {
                                        done.whenComplete((__, e) -> {
                                            if (e != null) {
                                                LOGGER.error("Error publishing events to kafka", e);
                                            } else {
                                                LOGGER.info("Closing publishing to {}", topic);
                                            }
                                        });
                                        return done;
                                    });
                        }
                )
                .viaMat(KillSwitches.single(), Keep.right())
                .toMat(Sink.ignore(), Keep.both())
                .run(materializer).first();
    }


    private <TxCtx> Flow<EventEnvelope<E, Meta, Context>, EventEnvelope<E, Meta, Context>, NotUsed> publishToKafka(EventStore<TxCtx, E, Meta, Context> eventStore, Option<TxCtx> tx, Flow<ProducerMessage.Results<String, EventEnvelope<E, Meta, Context>, EventEnvelope<E, Meta, Context>>, List<ProducerMessage.Results<String, EventEnvelope<E, Meta, Context>, EventEnvelope<E, Meta, Context>>>, NotUsed> groupFlow) {
        Flow<ProducerMessage.Envelope<String, EventEnvelope<E, Meta, Context>, EventEnvelope<E, Meta, Context>>, ProducerMessage.Results<String, EventEnvelope<E, Meta, Context>, EventEnvelope<E, Meta, Context>>, NotUsed> publishToKafkaFlow = Producer.<String, EventEnvelope<E, Meta, Context>, EventEnvelope<E, Meta, Context>>flexiFlow(producerSettings);
        return Flow.<EventEnvelope<E, Meta, Context>>create()
                .map(this::toKafkaMessage)
                .via(publishToKafkaFlow)
                .via(groupFlow)
                .mapAsync(1, m ->
                        tx.fold(
                                () -> eventStore.markAsPublished(m.map(ProducerMessage.Results::passThrough)).toCompletableFuture(),
                                txCtx -> eventStore.markAsPublished(txCtx, m.map(ProducerMessage.Results::passThrough)).toCompletableFuture()
                        )

                )
                .mapConcat(e -> e);
    }

    @Override
    public Future<Tuple0> publish(List<EventEnvelope<E, Meta, Context>> events) {
        LOGGER.debug("Publishing event in memory : \n{} ", events);
        return Future.fromCompletableFuture(
                Source
                        .from(events)
                        .mapAsync(1, queue::offer)
                        .runWith(Sink.ignore(), materializer)
                        .toCompletableFuture()
        ).map(__ -> Tuple.empty());
    }

    @Override
    public void close() throws IOException {
        if(Objects.nonNull(killSwitch)) {
            this.killSwitch.shutdown();
        }
        this.kafkaProducer.close();
    }


    private ProducerMessage.Envelope<String, EventEnvelope<E, Meta, Context>, EventEnvelope<E, Meta, Context>> toKafkaMessage(EventEnvelope<E, Meta, Context> eventEnvelope) {
        LOGGER.debug("Publishing to kafka topic {} : \n{}", topic, eventEnvelope);
        return new ProducerMessage.Message<>(
                new ProducerRecord<>(
                        topic,
                        eventEnvelope.event.hash(),
                        eventEnvelope
                ),
                eventEnvelope
        );
    }


    private <Any> Sink<Any, NotUsed> logProgress(int every) {
        return Flow.<Any>create()
                .scan(0, (acc, elt) -> acc + 1)
                .to(Sink.foreach(count -> {
                    if (count % every == 0) {
                        LOGGER.info("Replayed {} events on {}", count, topic);
                    }
                }));
    }

}
