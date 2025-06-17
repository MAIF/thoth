package fr.maif.eventsourcing.vanilla;

import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.State;
import fr.maif.eventsourcing.TransactionManager;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.Traversable;
import io.vavr.control.Option;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

public abstract class AbstractDefaultAggregateStore<S extends State<S>, E extends Event, Meta, Context, TxCtx> implements AggregateStore<S, String, TxCtx> {

    private final EventStore<TxCtx, E, Meta, Context> eventStore;
    private final EventHandler<S, E> eventEventHandler;
    private final TransactionManager<TxCtx> transactionManager;

    public AbstractDefaultAggregateStore(EventStore<TxCtx, E, Meta, Context> eventStore, EventHandler<S, E> eventEventHandler, TransactionManager<TxCtx> transactionManager) {
        this.eventStore = eventStore;
        this.eventEventHandler = eventEventHandler;
        this.transactionManager = transactionManager;
    }

    @Override
    public CompletionStage<Optional<S>> getAggregate(String entityId) {
        return transactionManager.withTransaction(ctx -> getAggregate(ctx, entityId));
    }

    @Override
    public CompletionStage<Map<String, Optional<S>>> getAggregates(TxCtx ctx, List<String> entityIds) {
        return this.getSnapshots(ctx, entityIds)
                .thenCompose(snapshots -> {
                    io.vavr.collection.Map<String, S> indexed = io.vavr.collection.List.ofAll(snapshots).groupBy(State::entityId).mapValues(Traversable::head);
                    io.vavr.collection.List<String> entityIdsVavr = io.vavr.collection.List.ofAll(entityIds);
                    io.vavr.collection.List<EventStore.IdAndSequence> idsAndSeqNums = entityIdsVavr.map(id -> new EventStore.IdAndSequence(id, indexed.get(id).map(s -> s.sequenceNum()).getOrElse(0L)));
                    io.vavr.collection.Map<String, Optional<S>> empty = HashMap.ofEntries(entityIdsVavr.map(id -> Tuple.of(id, indexed.get(id).toJavaOptional())));
                    EventStore.Query query = EventStore.Query.builder().withIdsAndSequences(idsAndSeqNums.toJavaList()).build();
                    Publisher<EventEnvelope<E, Meta, Context>> events = this.eventStore.loadEventsByQuery(ctx, query);
                    CompletionStage<io.vavr.collection.Map<String, Optional<S>>> result = fold(events,
                            empty,
                            (io.vavr.collection.Map<String, Optional<S>> states, EventEnvelope<E, Meta, Context> event) -> {
                                Optional<S> mayBeCurrentState = states.get(event.entityId).flatMap(Option::ofOptional).toJavaOptional();
                                Optional<S> newState = this.eventEventHandler
                                        .applyEvent(mayBeCurrentState, event.event)
                                        .map((S state) -> (S) state.withSequenceNum(event.sequenceNum));

                                return states.put(
                                        event.entityId,
                                        newState
                                );
                            }
                    );
                    return result;
                })
                .thenApply(m -> m.toJavaMap());
    }


    public CompletionStage<Optional<S>> getAggregate(TxCtx ctx, String entityId) {

        return this.getSnapshot(ctx, entityId)
                .thenCompose(mayBeSnapshot -> {
                    EventStore.Query query = mayBeSnapshot
                            // If a snapshot is defined, we read events from seq num of the snapshot :
                            .map(s -> EventStore.Query.builder().withSequenceFrom(s.sequenceNum()).withEntityId(entityId).build())
                            .orElseGet(
                            // No snapshot defined, we read all the events
                            () -> EventStore.Query.builder().withEntityId(entityId).build());

                    Publisher<EventEnvelope<E, Meta, Context>> events = this.eventStore.loadEventsByQuery(ctx, query);
                    return fold(events,
                            mayBeSnapshot,
                            (Optional<S> mayBeState, EventEnvelope<E, Meta, Context> event) ->
                                            this.eventEventHandler.applyEvent(mayBeState, event.event)
                                                    .map((S state) -> (S) state.withSequenceNum(event.sequenceNum))
                    );
                });
    }

    @Override
    public CompletionStage<Optional<S>> getPreviousAggregate(TxCtx ctx, Long sequenceNum, String entityId) {

        return this.getSnapshot(ctx, entityId)
                .thenCompose(mayBeSnapshot -> {

                    EventStore.Query query = mayBeSnapshot
                            .map(// If a snapshot is defined, we read events from seq num of the snapshot :
                                    s -> {
                                        if (s.sequenceNum() <= sequenceNum) {
                                            return EventStore.Query.builder().withSequenceFrom(s.sequenceNum()).withSequenceTo(sequenceNum).withEntityId(entityId).build();
                                        } else {
                                            return EventStore.Query.builder().withEntityId(entityId).withSequenceTo(sequenceNum).build();
                                        }
                                    })
                            .orElseGet(
                                // No snapshot defined, we read all the events
                                () -> EventStore.Query.builder().withEntityId(entityId).withSequenceTo(sequenceNum).build()
                            );

                    Publisher<EventEnvelope<E, Meta, Context>> events = this.eventStore.loadEventsByQuery(ctx, query);
                    return fold(events,
                            mayBeSnapshot,
                            (Optional<S> mayBeState, EventEnvelope<E, Meta, Context> event) -> {
                                if (event.sequenceNum < sequenceNum) {
                                    return this.eventEventHandler
                                            .applyEvent(mayBeState, event.event)
                                            .map(state -> state.withSequenceNum(event.sequenceNum));
                                } else {
                                    return mayBeState;
                                }
                            }
                    );
                });
    }



    protected abstract <T, A> CompletionStage<T> fold(Publisher<A> publisher, T empty, BiFunction<T, A, T> acc);
}
