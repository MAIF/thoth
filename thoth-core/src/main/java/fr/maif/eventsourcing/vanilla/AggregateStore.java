package fr.maif.eventsourcing.vanilla;

import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventHandler;
import fr.maif.eventsourcing.State;

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
    CompletionStage<Void> storeSnapshot(TxCtx transactionContext, Id id, Optional<S> state);

    /**
     * Get the snapshot for an aggregate id
     * @param transactionContext
     * @param id
     * @return The aggregate or empty
     */
    CompletionStage<Optional<S>> getSnapshot(TxCtx transactionContext, Id id);

    /**
     * Get the snapshots for aggregates ids
     * @param transactionContext
     * @param ids
     * @return The aggregate or empty
     */
    CompletionStage<List<S>> getSnapshots(TxCtx transactionContext, List<Id> ids);

    <E extends Event> CompletionStage<Optional<S>> buildAggregateAndStoreSnapshot(TxCtx ctx, EventHandler<S, E> eventHandler, Optional<S> state, Id id, List<E> events, Optional<Long> lastSequenceNum);

}
