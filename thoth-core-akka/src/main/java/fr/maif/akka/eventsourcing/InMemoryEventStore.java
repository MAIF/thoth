package fr.maif.akka.eventsourcing;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.Materializer;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.*;
import fr.maif.concurrent.CompletionStages;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventPublisher;
import fr.maif.eventsourcing.EventStore;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.control.Option;
import org.reactivestreams.Publisher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryEventStore<E extends Event, Meta, Context> implements EventStore<Tuple0, E, Meta, Context> {

    private final ActorSystem system;
    private final Materializer materializer;

    private java.util.List<EventEnvelope<E, Meta, Context>> eventStore = new ArrayList<>();

    private final SourceQueueWithComplete<EventEnvelope> queue;
    private final Source<EventEnvelope, NotUsed> realTimeEvents;

    private AtomicLong sequence_num = new AtomicLong(0);

    private final ConcurrentHashMap<String, Long> offsets = new ConcurrentHashMap<>();

    public InMemoryEventStore(ActorSystem system) {
        this.system = system;
        this.materializer = Materializer.createMaterializer(system);

        Pair<SourceQueueWithComplete<EventEnvelope>, Source<EventEnvelope, NotUsed>> run = Source
                .<EventEnvelope>queue(500, OverflowStrategy.backpressure())
                .toMat(BroadcastHub.of(EventEnvelope.class, 256), Keep.both())
                .run(materializer);

        this.queue = run.first();
        this.realTimeEvents = run.second();
        this.realTimeEvents.runWith(Sink.ignore(), materializer);
    }

    public static <E extends Event, Meta, Context> InMemoryEventStore<E, Meta, Context> create(ActorSystem system) {
        return new InMemoryEventStore<>(system);
    }

    @Override
    public CompletionStage<Long> lastPublishedSequence() {
        return CompletionStages.completedStage(eventStore.stream().filter(e -> e.published).map(e -> e.sequenceNum)
                .max(Comparator.comparingLong(e -> e))
                .orElse(0L));
    }

    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadEventsUnpublished(Tuple0 tx, ConcurrentReplayStrategy concurrentReplayStrategy) {
        return Source.<EventEnvelope<E, Meta, Context>>empty().runWith(Sink.asPublisher(AsPublisher.WITHOUT_FANOUT), system);
    }

    @Override
    public CompletionStage<EventEnvelope<E, Meta, Context>> markAsPublished(Tuple0 tx, EventEnvelope<E, Meta, Context> eventEnvelope) {
        return markAsPublished(eventEnvelope);
    }

    @Override
    public CompletionStage<Tuple0> openTransaction() {
        return CompletionStages.empty();
    }

    @Override
    public CompletionStage<Tuple0> commitOrRollback(Option<Throwable> of, Tuple0 tx) {
        return CompletableFuture.supplyAsync(Tuple::empty);
    }

    @Override
    public CompletionStage<EventEnvelope<E, Meta, Context>> markAsPublished(EventEnvelope<E, Meta, Context> eventEnvelope) {
        return CompletionStages.completedStage(
                eventEnvelope.copy().withPublished(true).build()
        );
    }

    @Override
    public CompletionStage<Long> nextSequence(Tuple0 tx) {
        return CompletionStages.completedStage(sequence_num.incrementAndGet());
    }

    @Override
    public CompletionStage<Tuple0> publish(List<EventEnvelope<E, Meta, Context>> events) {
        events.forEach(queue::offer);
        return CompletableFuture.supplyAsync(Tuple::empty);
    }

    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadEvents(String id) {
        return Source.from(eventStore).runWith(Sink.asPublisher(AsPublisher.WITHOUT_FANOUT), system);
    }


    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadAllEvents() {
        return Source.from(eventStore).runWith(Sink.asPublisher(AsPublisher.WITHOUT_FANOUT), system);
    }

    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadEventsByQuery(Tuple0 tx, Query query) {
        return loadEventsByQuery(query);
    }

    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadEventsByQuery(Query query) {
        return Source.from(eventStore)
                .filter(e -> {
                    return Option.of(query.entityId).map(id -> id.equals(e.entityId)).getOrElse(true);
                })
                .runWith(Sink.asPublisher(AsPublisher.WITHOUT_FANOUT), system);
    }

    @Override
    public CompletionStage<Tuple0> persist(Tuple0 transactionContext, List<EventEnvelope<E, Meta, Context>> events) {
        eventStore.addAll(events.toJavaList());
        return CompletableFuture.supplyAsync(Tuple::empty);
    }

    @Override
    public EventPublisher<E, Meta, Context> eventPublisher() {
        var _this = this;
        return new EventPublisher<E, Meta, Context>() {
            @Override
            public CompletionStage<Tuple0> publish(List<EventEnvelope<E, Meta, Context>> events) {
                return _this.publish(events);
            }

            @Override
            public void close() throws IOException {

            }
        };
    }
}
