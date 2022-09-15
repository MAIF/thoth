package fr.maif.eventsourcing;

import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;
import io.vavr.control.Option;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public interface AggregateStore<S extends State<S>, Id, TxCtx> {

    CompletionStage<Option<S>> getAggregate(Id entityId);

    CompletionStage<Option<S>> getAggregate(TxCtx ctx, Id entityId);

    default CompletionStage<Tuple0> storeSnapshot(TxCtx transactionContext, Id id, Option<S> state) {
        return CompletableFuture.completedStage(Tuple.empty());
    }

    default CompletionStage<Option<S>> getSnapshot(TxCtx transactionContext, Id id) {
        return CompletableFuture.completedStage(Option.none());
    }

    default <E extends Event> CompletionStage<Option<S>> buildAggregateAndStoreSnapshot(TxCtx ctx, EventHandler<S, E> eventHandler, Option<S> state, Id id, List<E> events, Option<Long> lastSequenceNum) {

        Option<S> newState = eventHandler.deriveState(state, events.filter(event -> event.entityId().equals(id)));

        Option<S> newStatewithSequence = lastSequenceNum
                .map(num -> newState.map(s -> (S) s.withSequenceNum(num)))
                .getOrElse(newState);

        return storeSnapshot(ctx, id, newStatewithSequence).thenApply(__ -> newState);
    }

}
