package fr.maif.eventsourcing;

import io.vavr.Tuple0;
import io.vavr.collection.List;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletionStage;

public interface ReactorProjection<TxCtx, E extends Event, Meta, Context> extends ProjectionGetter<TxCtx, E, Meta, Context> {

    Mono<Tuple0> storeProjection(TxCtx ctx, List<EventEnvelope<E, Meta, Context>> events);

    default Projection<TxCtx, E, Meta, Context> projection() {
        var _this = this;
        return new Projection<TxCtx, E, Meta, Context>() {
            @Override
            public CompletionStage<Tuple0> storeProjection(TxCtx txCtx, List<EventEnvelope<E, Meta, Context>> events) {
                return _this.storeProjection(txCtx, events).toFuture();
            }
        };
    }
}
