package fr.maif.eventsourcing;

import fr.maif.concurrent.CompletionStages;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.control.Option;

import java.util.concurrent.CompletionStage;

public interface AggregateStore<S extends State<S>, Id, TxCtx> {

    /**
     * Return the aggregate for an id.
     * Events are streamed from database and fold onto the aggregate using event handler.
     *
     * @param entityId
     * @return The aggregate or empty
     */
    CompletionStage<Option<S>> getAggregate(Id entityId);

    /**
     * Get the aggregates for aggregates ids
     * @param ctx
     * @param entityIds
     * @return The aggregate or empty
     */
    default CompletionStage<Map<Id, Option<S>>> getAggregates(TxCtx ctx, List<Id> entityIds) {
        return CompletionStages.traverse(entityIds, id -> getAggregate(ctx, id).thenApply(agg -> Tuple.of(id, agg)))
                .thenApply(HashMap::ofEntries);
    }

    /**
     * Return the aggregate for an id.
     * Events are streamed from database and fold onto the aggregate using event handler.
     *
     * @param ctx the current transaction
     * @param entityId
     * @return The aggregate or empty
     */
    CompletionStage<Option<S>> getAggregate(TxCtx ctx, Id entityId);

    /**
     * Build the aggregate as it was just before the sequence num in param.
     *
     * @param ctx the current transaction
     * @param sequenceNum the sequence we want the aggregate before
     * @param entityId the id of the entity
     * @return The aggregate or empty
     */
    CompletionStage<Option<S>> getPreviousAggregate(TxCtx ctx, Long sequenceNum, String entityId);

    /**
     * Persist a snapshot in the database. By default, nothing is done. You need to use a snapshot store.
     * @param transactionContext
     * @param id
     * @param state
     * @return
     */
    default CompletionStage<Tuple0> storeSnapshot(TxCtx transactionContext, Id id, Option<S> state) {
        return CompletionStages.completedStage(Tuple.empty());
    }

    /**
     * Get the snapshot for an aggregate id
     * @param transactionContext
     * @param id
     * @return The aggregate or empty
     */
    default CompletionStage<Option<S>> getSnapshot(TxCtx transactionContext, Id id) {
        return CompletionStages.completedStage(Option.none());
    }

    /**
     * Get the snapshots for aggregates ids
     * @param transactionContext
     * @param ids
     * @return The aggregate or empty
     */
    default CompletionStage<List<S>> getSnapshots(TxCtx transactionContext, List<Id> ids) {
        return CompletionStages.completedStage(List.empty());
    }

    default <E extends Event> CompletionStage<Option<S>> buildAggregateAndStoreSnapshot(TxCtx ctx, EventHandler<S, E> eventHandler, Option<S> state, Id id, List<E> events, Option<Long> lastSequenceNum) {

        Option<S> newState = eventHandler.deriveState(state, events.filter(event -> event.entityId().equals(id)));

        Option<S> newStatewithSequence = lastSequenceNum
                .map(num -> newState.map(s -> (S) s.withSequenceNum(num)))
                .getOrElse(newState);

        return storeSnapshot(ctx, id, newStatewithSequence).thenApply(__ -> newStatewithSequence);
    }

}
