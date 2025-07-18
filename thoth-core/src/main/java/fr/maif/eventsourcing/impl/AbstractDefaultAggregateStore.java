package fr.maif.eventsourcing.impl;

import fr.maif.eventsourcing.*;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Traversable;
import io.vavr.control.Option;
import org.reactivestreams.Publisher;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import static java.util.function.Function.identity;

public abstract class AbstractDefaultAggregateStore<S extends State<S>, E extends Event, Meta, Context, TxCtx> implements AggregateStore<S, String, TxCtx> {

    private final EventStore<TxCtx, E, Meta, Context> eventStore;
    private final EventHandler<S, E> eventEventHandler;
    private final TransactionManager<TxCtx> transactionManager;
    private final ReadConcurrencyStrategy readConcurrencyStrategy;

    public AbstractDefaultAggregateStore(EventStore<TxCtx, E, Meta, Context> eventStore, EventHandler<S, E> eventEventHandler, TransactionManager<TxCtx> transactionManager, ReadConcurrencyStrategy readConcurrencyStrategy) {
        this.eventStore = eventStore;
        this.eventEventHandler = eventEventHandler;
        this.transactionManager = transactionManager;
        this.readConcurrencyStrategy = Objects.requireNonNullElse(readConcurrencyStrategy, ReadConcurrencyStrategy.NO_STRATEGY);
    }

    @Override
    public CompletionStage<Option<S>> getAggregate(String entityId) {
        return transactionManager.withTransaction(ctx -> getAggregate(ctx, entityId));
    }

    @Override
    public CompletionStage<Map<String, Option<S>>> getAggregates(TxCtx ctx, List<String> entityIds) {
        return this.getSnapshots(ctx, entityIds)
                .thenCompose(snapshots -> {
                    Map<String, S> indexed = snapshots.groupBy(State::entityId).mapValues(Traversable::head);
                    List<Tuple2<String, Long>> idsAndSeqNums = entityIds.map(id -> Tuple.of(id, indexed.get(id).map(s -> s.sequenceNum()).getOrElse(0L)));
                    Map<String, Option<S>> empty = HashMap.ofEntries(entityIds.map(id -> Tuple.of(id, indexed.get(id))));
                    Publisher<EventEnvelope<E, Meta, Context>> events = this.eventStore.loadEventsByIdsAndSeqNum(ctx, idsAndSeqNums, readConcurrencyStrategy);
                    return fold(events,
                            empty,
                            (Map<String, Option<S>> states, EventEnvelope<E, Meta, Context> event) -> {
                                Option<S> mayBeCurrentState = states.get(event.entityId).flatMap(identity());
                                return states.put(
                                        event.entityId,
                                        this.eventEventHandler
                                                .applyEvent(mayBeCurrentState, event.event)
                                                .map((S state) -> (S) state.withSequenceNum(event.sequenceNum))
                                );
                            }
                    );
                });
    }


    public CompletionStage<Option<S>> getAggregate(TxCtx ctx, String entityId) {

        return this.getSnapshot(ctx, entityId)
                .thenCompose(mayBeSnapshot -> {

                    EventStore.Query query = mayBeSnapshot.fold(
                            // No snapshot defined, we read all the events
                            () -> EventStore.Query.builder().withEntityId(entityId).build(),
                            // If a snapshot is defined, we read events from seq num of the snapshot :
                            s -> EventStore.Query.builder().withSequenceFrom(s.sequenceNum()).withEntityId(entityId).build()
                    );

                    Publisher<EventEnvelope<E, Meta, Context>> events = this.eventStore.loadEventsByQuery(ctx, query);
                    return fold(events,
                            mayBeSnapshot,
                            (Option<S> mayBeState, EventEnvelope<E, Meta, Context> event) ->
                                            this.eventEventHandler.applyEvent(mayBeState, event.event)
                                                    .map((S state) -> (S) state.withSequenceNum(event.sequenceNum))
                    );
                });
    }

    @Override
    public CompletionStage<Option<S>> getPreviousAggregate(TxCtx ctx, Long sequenceNum, String entityId) {

        return this.getSnapshot(ctx, entityId)
                .thenCompose(mayBeSnapshot -> {

                    EventStore.Query query = mayBeSnapshot.fold(
                            // No snapshot defined, we read all the events
                            () -> EventStore.Query.builder().withEntityId(entityId).withSequenceTo(sequenceNum).build(),
                            // If a snapshot is defined, we read events from seq num of the snapshot :
                            s -> {
                                if (s.sequenceNum() <= sequenceNum) {
                                    return EventStore.Query.builder().withSequenceFrom(s.sequenceNum()).withSequenceTo(sequenceNum).withEntityId(entityId).build();
                                } else {
                                    return EventStore.Query.builder().withEntityId(entityId).withSequenceTo(sequenceNum).build();
                                }
                            }
                    );

                    Publisher<EventEnvelope<E, Meta, Context>> events = this.eventStore.loadEventsByQuery(ctx, query);
                    return fold(events,
                            mayBeSnapshot,
                            (Option<S> mayBeState, EventEnvelope<E, Meta, Context> event) -> {
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
