package fr.maif.eventsourcing;

import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;

import java.util.concurrent.CompletionStage;

public interface Projection<TxCtx, E extends Event, Meta, Context> {

    CompletionStage<Tuple0> storeProjection(TxCtx ctx, List<EventEnvelope<E, Meta, Context>> events);

}
