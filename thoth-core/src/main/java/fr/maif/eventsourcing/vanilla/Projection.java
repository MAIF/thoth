package fr.maif.eventsourcing.vanilla;

import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.ProjectionGetter;
import io.vavr.Tuple;

import java.util.List;
import java.util.concurrent.CompletionStage;

public interface Projection<TxCtx, E extends Event, Meta, Context> extends ProjectionGetter<TxCtx, E, Meta, Context> {

    CompletionStage<Void> storeProjection(TxCtx ctx, List<EventEnvelope<E, Meta, Context>> events);

    @Override
    default fr.maif.eventsourcing.Projection<TxCtx, E, Meta, Context> projection() {
        var _this = this;
        return (txCtx, events) -> _this.storeProjection(txCtx, events.toJavaList()).thenApply(v -> Tuple.empty());
    }
}
