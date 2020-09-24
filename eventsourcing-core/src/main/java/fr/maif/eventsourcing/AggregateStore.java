package fr.maif.eventsourcing;

import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;
import io.vavr.control.Option;

public interface AggregateStore<S extends State<S>, Id, TxCtx> {

    Future<Option<S>> getAggregate(Id entityId);

    Future<Option<S>> getAggregate(TxCtx ctx, Id entityId);

    default Future<Tuple0> storeSnapshot(TxCtx transactionContext, Id id, Option<S> state) {
        return Future.successful(Tuple.empty());
    }

    default <E extends Event> Future<Option<S>> buildAggregateAndStoreSnapshot(TxCtx ctx, EventHandler<S, E> eventHandler, Option<S> state, Id id, List<E> events, Option<Long> lastSequenceNum) {

        Option<S> newState = eventHandler.deriveState(state, events.filter(event -> event.entityId().equals(id)));

        Option<S> newStatewithSequence = lastSequenceNum
                .map(num -> newState.map(s -> (S) s.withSequenceNum(num)))
                .getOrElse(newState);

        return storeSnapshot(ctx, id, newStatewithSequence).map(__ -> newState);
    }

}
