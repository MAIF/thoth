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

    CompletionStage<Option<S>> getAggregate(Id entityId);

    CompletionStage<Map<String, Option<S>>> getAggregates(TxCtx ctx, List<String> entityIds);

    CompletionStage<Option<S>> getAggregate(TxCtx ctx, Id entityId);

    default CompletionStage<Tuple0> storeSnapshot(TxCtx transactionContext, Id id, Option<S> state) {
        return CompletionStages.completedStage(Tuple.empty());
    }

    default CompletionStage<Option<S>> getSnapshot(TxCtx transactionContext, Id id) {
        return CompletionStages.completedStage(Option.none());
    }

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
