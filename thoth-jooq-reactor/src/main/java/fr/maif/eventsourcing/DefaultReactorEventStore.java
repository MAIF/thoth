package fr.maif.eventsourcing;

import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.control.Option;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DefaultReactorEventStore<TxCtx, E extends Event, Meta, Context> implements ReactorEventStore<TxCtx, E, Meta, Context> {

    private final EventStore<TxCtx, E, Meta, Context> eventStore;

    public DefaultReactorEventStore(EventStore<TxCtx, E, Meta, Context> eventStore) {
        this.eventStore = eventStore;
    }

    @Override
    public Mono<Tuple0> persist(TxCtx transactionContext, List<EventEnvelope<E, Meta, Context>> events) {
        return Mono.fromCompletionStage(() -> eventStore.persist(transactionContext, events));
    }

    @Override
    public Flux<EventEnvelope<E, Meta, Context>> loadEventsUnpublished(TxCtx tx, EventStore.ConcurrentReplayStrategy concurrentReplayStrategy) {
        return Flux.from(eventStore.loadEventsUnpublished(tx, concurrentReplayStrategy));
    }

    @Override
    public Flux<EventEnvelope<E, Meta, Context>> loadEventsByQuery(TxCtx tx, EventStore.Query query) {
        return Flux.from(eventStore.loadEventsByQuery(tx, query));
    }

    @Override
    public Flux<EventEnvelope<E, Meta, Context>> loadEventsByQuery(EventStore.Query query) {
        return Flux.from(eventStore.loadEventsByQuery(query));
    }

    @Override
    public Mono<Long> nextSequence(TxCtx tx) {
        return Mono.fromCompletionStage(() -> eventStore.nextSequence(tx));
    }

    @Override
    public Mono<Tuple0> publish(List<EventEnvelope<E, Meta, Context>> events) {
        return Mono.fromCompletionStage(() -> eventStore.publish(events));
    }

    @Override
    public Mono<EventEnvelope<E, Meta, Context>> markAsPublished(TxCtx tx, EventEnvelope<E, Meta, Context> eventEnvelope) {
        return Mono.fromCompletionStage(() -> eventStore.markAsPublished(tx, eventEnvelope));
    }

    @Override
    public Mono<EventEnvelope<E, Meta, Context>> markAsPublished(EventEnvelope<E, Meta, Context> eventEnvelope) {
        return Mono.fromCompletionStage(() -> eventStore.markAsPublished(eventEnvelope));
    }

    @Override
    public Mono<List<EventEnvelope<E, Meta, Context>>> markAsPublished(List<EventEnvelope<E, Meta, Context>> eventEnvelopes) {
        return Mono.fromCompletionStage(() -> eventStore.markAsPublished(eventEnvelopes));
    }

    @Override
    public Mono<List<EventEnvelope<E, Meta, Context>>> markAsPublished(TxCtx tx, List<EventEnvelope<E, Meta, Context>> eventEnvelopes) {
        return Mono.fromCompletionStage(() -> eventStore.markAsPublished(tx, eventEnvelopes));
    }

    @Override
    public Flux<EventEnvelope<E, Meta, Context>> loadEvents(String id) {
        return Flux.from(eventStore.loadEvents(id));
    }

    @Override
    public Flux<EventEnvelope<E, Meta, Context>> loadAllEvents() {
        return Flux.from(eventStore.loadAllEvents());
    }

    @Override
    public Mono<TxCtx> openTransaction() {
        return Mono.fromCompletionStage(() -> eventStore.openTransaction());
    }

    @Override
    public Mono<Tuple0> commitOrRollback(Option<Throwable> of, TxCtx tx) {
        return Mono.fromCompletionStage(() -> eventStore.commitOrRollback(of, tx));
    }

    @Override
    public EventStore<TxCtx, E, Meta, Context> toEventStore() {
        return eventStore;
    }
}
