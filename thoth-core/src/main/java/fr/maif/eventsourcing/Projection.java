package fr.maif.eventsourcing;

import io.vavr.collection.List;

import java.util.concurrent.CompletionStage;

public interface Projection<TxCtx, E extends Event, Meta, Context> extends ProjectionGetter<TxCtx, E, Meta, Context> {

    CompletionStage<Void> storeProjection(TxCtx ctx, List<EventEnvelope<E, Meta, Context>> events);

    @Override
    default Projection<TxCtx, E, Meta, Context> projection() {
        return this;
    }
}
