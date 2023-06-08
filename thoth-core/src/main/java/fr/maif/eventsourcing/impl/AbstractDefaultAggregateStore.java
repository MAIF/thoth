package fr.maif.eventsourcing.impl;

import fr.maif.eventsourcing.AggregateStore;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventHandler;
import fr.maif.eventsourcing.EventStore;
import fr.maif.eventsourcing.State;
import fr.maif.eventsourcing.TransactionManager;
import io.vavr.control.Option;
import org.reactivestreams.Publisher;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

public abstract class AbstractDefaultAggregateStore<S extends State<S>, E extends Event, Meta, Context, TxCtx> implements AggregateStore<S, String, TxCtx> {

    private final EventStore<TxCtx, E, Meta, Context> eventStore;
    private final EventHandler<S, E> eventEventHandler;
    private final TransactionManager<TxCtx> transactionManager;

    private final Boolean shouldLockEntityForUpdate;

    public AbstractDefaultAggregateStore(EventStore<TxCtx, E, Meta, Context> eventStore, EventHandler<S, E> eventEventHandler, TransactionManager<TxCtx> transactionManager, Boolean shouldLockEntityForUpdate) {
        this.eventStore = eventStore;
        this.eventEventHandler = eventEventHandler;
        this.transactionManager = transactionManager;
        this.shouldLockEntityForUpdate = Objects.requireNonNullElse(shouldLockEntityForUpdate, false);
    }

    @Override
    public CompletionStage<Option<S>> getAggregate(String entityId) {
        return transactionManager.withTransaction(ctx -> getAggregate(ctx, entityId));
    }

    @Override
    public CompletionStage<Option<S>> getAggregate(TxCtx ctx, String entityId) {

        return this.getSnapshot(ctx, entityId)
                .thenCompose(mayBeSnapshot -> {

                    EventStore.Query query = mayBeSnapshot.fold(
                            // No snapshot defined, we read all the events
                            () -> EventStore.Query.builder().withEntityId(entityId).withShouldLockEntity(this.shouldLockEntityForUpdate).build(),
                            // If a snapshot is defined, we read events from seq num of the snapshot :
                            s -> EventStore.Query.builder().withSequenceFrom(s.sequenceNum()).withEntityId(entityId).withShouldLockEntity(this.shouldLockEntityForUpdate).build()
                    );

                    return fold(this.eventStore.loadEventsByQuery(ctx, query),
                            mayBeSnapshot,
                            (Option<S> mayBeState, EventEnvelope<E, Meta, Context> event) ->
                                            this.eventEventHandler.applyEvent(mayBeState, event.event)
                                                    .map((S state) -> (S) state.withSequenceNum(event.sequenceNum))
                    );
                });
    }

    protected abstract <T, A> CompletionStage<T> fold(Publisher<A> publisher, T empty, BiFunction<T, A, T> acc);
}
