package fr.maif.eventsourcing;

import io.vavr.collection.List;
import io.vavr.control.Option;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;


public interface ReactorEventStore<TxCtx, E extends Event, Meta, Context> {

    Mono<Void> persist(TxCtx transactionContext, List<EventEnvelope<E, Meta, Context>> events);

    Flux<EventEnvelope<E, Meta, Context>> loadEventsUnpublished(TxCtx tx, EventStore.ConcurrentReplayStrategy concurrentReplayStrategy);

    Flux<EventEnvelope<E, Meta, Context>> loadEventsByQuery(TxCtx tx, EventStore.Query query);

    Flux<EventEnvelope<E, Meta, Context>> loadEventsByQuery(EventStore.Query query);

    default Flux<EventEnvelope<E, Meta, Context>> loadEvents(String id) {
        return loadEventsByQuery(EventStore.Query.builder().withEntityId(id).build());
    }

    default Flux<EventEnvelope<E, Meta, Context>> loadAllEvents() {
        return loadEventsByQuery(EventStore.Query.builder().build());
    }

    Mono<Long> nextSequence(TxCtx tx);

    Mono<Void> publish(List<EventEnvelope<E, Meta, Context>> events);

    Mono<EventEnvelope<E, Meta, Context>> markAsPublished(TxCtx tx, EventEnvelope<E, Meta, Context> eventEnvelope);

    default Mono<List<EventEnvelope<E, Meta, Context>>> markAsPublished(TxCtx tx, List<EventEnvelope<E, Meta, Context>> eventEnvelopes) {
        return Flux.fromIterable(eventEnvelopes)
                .concatMap(evt -> this.markAsPublished(tx, evt))
                .collectList()
                .map(List::ofAll);
    }

    Mono<EventEnvelope<E, Meta, Context>> markAsPublished(EventEnvelope<E, Meta, Context> eventEnvelope);

    default Mono<List<EventEnvelope<E, Meta, Context>>> markAsPublished(List<EventEnvelope<E, Meta, Context>> eventEnvelopes) {
        return Flux.fromIterable(eventEnvelopes)
                .concatMap(this::markAsPublished)
                .collectList()
                .map(List::ofAll);
    }

    Mono<TxCtx> openTransaction();

    Mono<Void> commitOrRollback(Option<Throwable> of, TxCtx tx);

    static <TxCtx, E extends Event, Meta, Context> ReactorEventStore<TxCtx, E, Meta, Context> fromEventStore(EventStore<TxCtx, E, Meta, Context> eventStore) {
        return new ReactorEventStore<TxCtx, E, Meta, Context>() {
            @Override
            public Mono<Void> persist(TxCtx transactionContext, List<EventEnvelope<E, Meta, Context>> events) {
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
            public Mono<Void> publish(List<EventEnvelope<E, Meta, Context>> events) {
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
            public Mono<TxCtx> openTransaction() {
                return Mono.fromCompletionStage(() -> eventStore.openTransaction());
            }

            @Override
            public Mono<Void> commitOrRollback(Option<Throwable> of, TxCtx tx) {
                return Mono.fromCompletionStage(() -> eventStore.commitOrRollback(of, tx));
            }
        };
    }

}
