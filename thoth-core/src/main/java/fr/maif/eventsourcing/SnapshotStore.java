package fr.maif.eventsourcing;

import io.vavr.Tuple0;
import io.vavr.concurrent.Future;
import io.vavr.control.Option;

import java.util.concurrent.CompletionStage;

/**
 * @deprecated  use AggregateStore and Projection instead
 */
@Deprecated
public interface SnapshotStore<S extends State<S>, Id, TxCtx> extends AggregateStore<S, Id, TxCtx> {

    CompletionStage<Option<S>> getSnapshot(TxCtx ctx, Id entityId);

    CompletionStage<Option<S>> getSnapshot(Id entityId);

    default CompletionStage<Option<S>> getAggregate(Id entityId) {
        return getSnapshot(entityId);
    }

    default CompletionStage<Option<S>> getAggregate(TxCtx ctx, Id entityId) {
        return getSnapshot(ctx, entityId);
    }

    CompletionStage<Tuple0> persist(TxCtx transactionContext, Id id, Option<S> state) ;

    @Override
    default CompletionStage<Tuple0> storeSnapshot(TxCtx transactionContext, Id id, Option<S> state) {
        return persist(transactionContext, id, state);
    }
}
