package fr.maif.eventsourcing;

import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;

public interface Projection<TxCtx, E extends Event, Meta, Context> {

    Future<Tuple0> storeProjection(TxCtx ctx, List<EventEnvelope<E, Meta, Context>> events);

}
