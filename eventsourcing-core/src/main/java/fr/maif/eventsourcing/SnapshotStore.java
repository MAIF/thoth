package fr.maif.eventsourcing;

import io.vavr.Tuple0;
import io.vavr.concurrent.Future;
import io.vavr.control.Option;

/**
 * @deprecated  use AggregateStore and Projection instead
 */
@Deprecated
public interface SnapshotStore<S extends State<S>, Id, TxCtx> extends AggregateStore<S, Id, TxCtx> {

    Future<Option<S>> getSnapshot(TxCtx ctx, Id entityId);

    Future<Option<S>> getSnapshot(Id entityId);

    default Future<Option<S>> getAggregate(Id entityId) {
        return getSnapshot(entityId);
    }

    default Future<Option<S>> getAggregate(TxCtx ctx, Id entityId) {
        return getSnapshot(ctx, entityId);
    }

    Future<Tuple0> persist(TxCtx transactionContext, Id id, Option<S> state) ;

    @Override
    default Future<Tuple0> storeSnapshot(TxCtx transactionContext, Id id, Option<S> state) {
        return persist(transactionContext, id, state);
    }
}
