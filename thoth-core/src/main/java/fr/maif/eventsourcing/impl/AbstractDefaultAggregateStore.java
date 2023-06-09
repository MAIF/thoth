package fr.maif.eventsourcing.impl;

import fr.maif.eventsourcing.*;
import io.vavr.control.Option;
import org.reactivestreams.Publisher;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

public abstract class AbstractDefaultAggregateStore<S extends State<S>, E extends Event, Meta, Context, TxCtx> implements AggregateStore<S, String, TxCtx> {

    private final AutoSnapshotingStrategy autoSnapshotingStrategy;
    private final EventStore<TxCtx, E, Meta, Context> eventStore;
    private final EventHandler<S, E> eventEventHandler;
    private final TransactionManager<TxCtx> transactionManager;

    public AbstractDefaultAggregateStore(AutoSnapshotingStrategy autoSnapshotingStrategy, EventStore<TxCtx, E, Meta, Context> eventStore, EventHandler<S, E> eventEventHandler, TransactionManager<TxCtx> transactionManager) {
        this.autoSnapshotingStrategy = autoSnapshotingStrategy;
        this.eventStore = eventStore;
        this.eventEventHandler = eventEventHandler;
        this.transactionManager = transactionManager;
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
                            () -> EventStore.Query.builder().withEntityId(entityId).build(),
                            // If a snapshot is defined, we read events from seq num of the snapshot :
                            s -> EventStore.Query.builder().withSequenceFrom(s.sequenceNum()).withEntityId(entityId).build()
                    );
                    AtomicInteger eventCount = new AtomicInteger(0);
                    return fold(this.eventStore.loadEventsByQuery(ctx, query),
                            mayBeSnapshot,
                            (Option<S> mayBeState, EventEnvelope<E, Meta, Context> event) -> {
                                eventCount.incrementAndGet();
                                return this.eventEventHandler.applyEvent(mayBeState, event.event)
                                        .map((S state) -> (S) state.withSequenceNum(event.sequenceNum));
                            }
                    ).thenCompose(mayBeAggregate -> {
                        if (autoSnapshotingStrategy.shouldSnapshot(eventCount.get())) {
                            return this.storeSnapshot(ctx, entityId, mayBeAggregate)
                                    .thenApply(__ -> mayBeAggregate)
                                    .exceptionally(e -> mayBeAggregate);
                        }
                        return CompletableFuture.completedStage(mayBeAggregate);
                    });
                });
    }

    protected abstract <T, A> CompletionStage<T> fold(Publisher<A> publisher, T empty, BiFunction<T, A, T> acc);
}
