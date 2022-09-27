package fr.maif.reactor.eventsourcing;

import fr.maif.concurrent.CompletionStages;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventStore;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.control.Option;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryEventStore<E extends Event, Meta, Context> implements EventStore<Tuple0, E, Meta, Context> {

    private java.util.List<EventEnvelope<E, Meta, Context>> eventStore = new ArrayList<>();

    private final Sinks.Many<EventEnvelope<E, Meta, Context>> queue;
    private final Flux<EventEnvelope<E, Meta, Context>> realTimeEvents;

    private AtomicLong sequence_num = new AtomicLong(0);

    private final ConcurrentHashMap<String, Long> offsets = new ConcurrentHashMap<>();

    public InMemoryEventStore() {
        this.queue = Sinks.many().multicast().onBackpressureBuffer(10000);
        this.realTimeEvents = queue.asFlux();
    }

    public static <E extends Event, Meta, Context> InMemoryEventStore<E, Meta, Context> create() {
        return new InMemoryEventStore<>();
    }

    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadEventsUnpublished(Tuple0 tx, ConcurrentReplayStrategy concurrentReplayStrategy) {
        return Flux.empty();
    }

    @Override
    public CompletionStage<EventEnvelope<E, Meta, Context>> markAsPublished(Tuple0 tx, EventEnvelope<E, Meta, Context> eventEnvelope) {
        return markAsPublished(eventEnvelope);
    }

    @Override
    public CompletionStage<Tuple0> openTransaction() {
        return CompletableFuture.completedStage(Tuple.empty());
    }

    @Override
    public CompletionStage<Void> commitOrRollback(Option<Throwable> of, Tuple0 tx) {
        return CompletionStages.empty();
    }

    @Override
    public CompletionStage<EventEnvelope<E, Meta, Context>> markAsPublished(EventEnvelope<E, Meta, Context> eventEnvelope) {
        return CompletableFuture.completedStage(
                eventEnvelope.copy().withPublished(true).build()
        );
    }

    @Override
    public CompletionStage<Long> nextSequence(Tuple0 tx) {
        return CompletableFuture.completedStage(sequence_num.incrementAndGet());
    }

    @Override
    public CompletionStage<Void> publish(List<EventEnvelope<E, Meta, Context>> events) {
        events.forEach(queue::tryEmitNext);
        return CompletionStages.empty();
    }

    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadEvents(String id) {
        return Flux.fromIterable(eventStore);
    }


    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadAllEvents() {
        return Flux.fromIterable(eventStore);
    }

    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadEventsByQuery(Tuple0 tx, Query query) {
        return loadEventsByQuery(query);
    }

    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadEventsByQuery(Query query) {
        return Flux.fromIterable(eventStore)
                .filter(e -> Option.of(query.entityId).map(id -> id.equals(e.entityId)).getOrElse(true));
    }

    @Override
    public CompletionStage<Void> persist(Tuple0 transactionContext, List<EventEnvelope<E, Meta, Context>> events) {
        eventStore.addAll(events.toJavaList());
        return CompletionStages.empty();
    }
}
