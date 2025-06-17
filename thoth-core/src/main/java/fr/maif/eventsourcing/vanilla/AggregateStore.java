package fr.maif.eventsourcing.vanilla;

import fr.maif.concurrent.CompletionStages;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.State;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.HashMap;
import io.vavr.control.Option;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public interface AggregateStore<S extends State<S>, Id, TxCtx> {

    /**
     * Return the aggregate for an id.
     * Events are streamed from database and fold onto the aggregate using event handler.
     *
     * @param entityId
     * @return The aggregate or empty
     */
    CompletionStage<Optional<S>> getAggregate(Id entityId);

    /**
     * Get the aggregates for aggregates ids
     * @param ctx
     * @param entityIds
     * @return The aggregate or empty
     */
    CompletionStage<Map<Id, Optional<S>>> getAggregates(TxCtx ctx, List<Id> entityIds);

    /**
     * Return the aggregate for an id.
     * Events are streamed from database and fold onto the aggregate using event handler.
     *
     * @param ctx the current transaction
     * @param entityId
     * @return The aggregate or empty
     */
    CompletionStage<Optional<S>> getAggregate(TxCtx ctx, Id entityId);

    /**
     * Build the aggregate as it was just before the sequence num in param.
     *
     * @param ctx the current transaction
     * @param sequenceNum the sequence we want the aggregate before
     * @param entityId the id of the entity
     * @return The aggregate or empty
     */
    CompletionStage<Optional<S>> getPreviousAggregate(TxCtx ctx, Long sequenceNum, String entityId);

    /**
     * Persist a snapshot in the database. By default, nothing is done. You need to use a snapshot store.
     * @param transactionContext
     * @param id
     * @param state
     * @return
     */
    default CompletionStage<Void> storeSnapshot(TxCtx transactionContext, Id id, Optional<S> state){
        return CompletionStages.completedStage("").thenAccept(any -> {});
    }

    /**
     * Get the snapshot for an aggregate id
     * @param transactionContext
     * @param id
     * @return The aggregate or empty
     */
    default CompletionStage<Optional<S>> getSnapshot(TxCtx transactionContext, Id id) {
        return CompletionStages.completedStage(Optional.empty());
    }

    /**
     * Get the snapshots for aggregates ids
     * @param transactionContext
     * @param ids
     * @return The aggregate or empty
     */
    default CompletionStage<List<S>> getSnapshots(TxCtx transactionContext, List<Id> ids) {
        return CompletionStages.completedStage(List.of());
    }

    default <E extends Event> CompletionStage<Optional<S>> buildAggregateAndStoreSnapshot(TxCtx ctx, EventHandler<S, E> eventHandler, Optional<S> state, Id id, List<E> events, Optional<Long> lastSequenceNum) {
        Optional<S> newState = eventHandler.deriveState(state, events.stream().filter(event -> event.entityId().equals(id)).toList());

        Optional<S> newStatewithSequence = lastSequenceNum
                .map(num -> newState.map(s -> (S) s.withSequenceNum(num)))
                .orElse(newState);

        return storeSnapshot(ctx, id, newStatewithSequence).thenApply(__ -> newStatewithSequence);
    }

    default fr.maif.eventsourcing.AggregateStore<S, Id, TxCtx> toAggregateStore() {
        var _this = this;
        return new fr.maif.eventsourcing.AggregateStore<>() {
            @Override
            public CompletionStage<Option<S>> getAggregate(Id entityId) {
                return _this.getAggregate(entityId).thenApply(Option::ofOptional);
            }

            @Override
            public CompletionStage<Option<S>> getAggregate(TxCtx txCtx, Id entityId) {
                return _this.getAggregate(txCtx, entityId).thenApply(Option::ofOptional);
            }

            @Override
            public CompletionStage<Option<S>> getPreviousAggregate(TxCtx txCtx, Long sequenceNum, String entityId) {
                return _this.getPreviousAggregate(txCtx, sequenceNum, entityId).thenApply(Option::ofOptional);
            }

            @Override
            public CompletionStage<io.vavr.collection.Map<Id, Option<S>>> getAggregates(TxCtx txCtx, io.vavr.collection.List<Id> entityIds) {
                return _this.getAggregates(txCtx, entityIds.toJavaList()).thenApply(m -> HashMap.ofAll(m).mapValues(Option::ofOptional));
            }

            @Override
            public CompletionStage<Tuple0> storeSnapshot(TxCtx transactionContext, Id id, Option<S> state) {
                return _this.storeSnapshot(transactionContext, id, state.toJavaOptional()).thenApply(any -> Tuple.empty());
            }

            @Override
            public CompletionStage<Option<S>> getSnapshot(TxCtx transactionContext, Id id) {
                return _this.getSnapshot(transactionContext, id).thenApply(Option::ofOptional);
            }

            @Override
            public CompletionStage<io.vavr.collection.List<S>> getSnapshots(TxCtx transactionContext, io.vavr.collection.List<Id> ids) {
                return _this.getSnapshots(transactionContext, ids.toJavaList()).thenApply(io.vavr.collection.List::ofAll);
            }

            @Override
            public <E extends Event> CompletionStage<Option<S>> buildAggregateAndStoreSnapshot(TxCtx txCtx, fr.maif.eventsourcing.EventHandler<S, E> eventHandler, Option<S> state, Id id, io.vavr.collection.List<E> events, Option<Long> lastSequenceNum) {
                return _this.buildAggregateAndStoreSnapshot(txCtx,
                        EventHandler.from(eventHandler),
                        state.toJavaOptional(),
                        id,
                        events.toJavaList(),
                        lastSequenceNum.toJavaOptional()).thenApply(Option::ofOptional);
            }
        };
    }

}
