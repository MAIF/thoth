package fr.maif.eventsourcing.vanilla;

import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventHandler;
import fr.maif.eventsourcing.State;
import io.vavr.control.Option;

import java.sql.Connection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public class AggregateStoreVanilla<S extends State<S>, Id, TxCtx> implements AggregateStore<S, String, TxCtx> {
    private final fr.maif.eventsourcing.AggregateStore<S, String, TxCtx> aggregateStore;

    public AggregateStoreVanilla(fr.maif.eventsourcing.AggregateStore<S, String, TxCtx> aggregateStore) {
        this.aggregateStore = aggregateStore;
    }

    @Override
    public CompletionStage<Optional<S>> getAggregate(String entityId) {
        return aggregateStore.getAggregate(entityId).thenApply(opt -> opt.toJavaOptional());
    }

    @Override
    public CompletionStage<Map<String, Optional<S>>> getAggregates(TxCtx connection, java.util.List<String> entityIds) {
        return aggregateStore.getAggregates(connection, io.vavr.collection.List.ofAll(entityIds)).thenApply(m -> m.mapValues(Option::toJavaOptional).toJavaMap());
    }

    @Override
    public CompletionStage<Optional<S>> getAggregate(TxCtx connection, String entityId) {
        return aggregateStore.getAggregate(connection, entityId).thenApply(opt -> opt.toJavaOptional());
    }

    @Override
    public CompletionStage<Optional<S>> getPreviousAggregate(TxCtx connection, Long sequenceNum, String entityId) {
        return aggregateStore.getPreviousAggregate(connection, sequenceNum, entityId).thenApply(opt -> opt.toJavaOptional());
    }

    @Override
    public CompletionStage<Void> storeSnapshot(TxCtx transactionContext, String s, Optional<S> state) {
        return aggregateStore.storeSnapshot(transactionContext, s, Option.ofOptional(state)).thenAccept(any -> {});
    }

    @Override
    public CompletionStage<Optional<S>> getSnapshot(TxCtx transactionContext, String s) {
        return aggregateStore.getSnapshot(transactionContext, s).thenApply(opt -> opt.toJavaOptional());
    }

    @Override
    public CompletionStage<java.util.List<S>> getSnapshots(TxCtx transactionContext, java.util.List<String> strings) {
        return aggregateStore.getSnapshots(transactionContext, io.vavr.collection.List.ofAll(strings)).thenApply(opt -> opt.toJavaList());
    }

    @Override
    public <E extends Event> CompletionStage<Optional<S>> buildAggregateAndStoreSnapshot(TxCtx connection, EventHandler<S, E> eventHandler, Optional<S> state, String s, java.util.List<E> events, Optional<Long> lastSequenceNum) {
        return aggregateStore.buildAggregateAndStoreSnapshot(connection, eventHandler, Option.ofOptional(state), s, io.vavr.collection.List.ofAll(events), Option.ofOptional(lastSequenceNum))
                .thenApply(opt -> opt.toJavaOptional());
    }
}
