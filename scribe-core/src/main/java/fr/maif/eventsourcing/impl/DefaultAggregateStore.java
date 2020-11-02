package fr.maif.eventsourcing.impl;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import fr.maif.eventsourcing.*;
import io.vavr.concurrent.Future;
import io.vavr.control.Option;

public class DefaultAggregateStore<S extends State<S>, E extends Event, Meta, Context, TxCtx> implements AggregateStore<S, String, TxCtx> {

    private final EventStore<TxCtx, E, Meta, Context> eventStore;
    private final EventHandler<S, E> eventEventHandler;
    private final Materializer materializer;
    private final TransactionManager<TxCtx> transactionManager;


    public DefaultAggregateStore(EventStore<TxCtx, E, Meta, Context> eventStore, EventHandler<S, E> eventEventHandler, ActorSystem system, TransactionManager<TxCtx> transactionManager) {
        this(eventStore, eventEventHandler, Materializer.createMaterializer(system), transactionManager);
    }

    public DefaultAggregateStore(EventStore<TxCtx, E, Meta, Context> eventStore, EventHandler<S, E> eventEventHandler, Materializer materializer, TransactionManager<TxCtx> transactionManager) {
        this.eventStore = eventStore;
        this.eventEventHandler = eventEventHandler;
        this.materializer = materializer;
        this.transactionManager = transactionManager;
    }

    @Override
    public Future<Option<S>> getAggregate(String entityId) {
        return transactionManager.withTransaction(ctx -> getAggregate(ctx, entityId));
    }

    @Override
    public Future<Option<S>> getAggregate(TxCtx ctx, String entityId) {
        return Future.fromCompletableFuture(this.eventStore
                .loadEventsByQuery(ctx, EventStore.Query.builder().withEntityId(entityId).build())
                .runFold(Option.<S>none(), (mayBeState, event) ->
                                this.eventEventHandler.applyEvent(mayBeState, event.event)
                                        .map((S state) -> (S) state.withSequenceNum(event.sequenceNum))
                        , materializer)
                .toCompletableFuture()
        );
    }
}
