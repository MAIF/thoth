package fr.maif.eventsourcing.impl;

import fr.maif.eventsourcing.AggregateStore;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventHandler;
import fr.maif.eventsourcing.EventStore;
import fr.maif.eventsourcing.State;
import fr.maif.eventsourcing.TransactionManager;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

public class DefaultAggregateStore<S extends State<S>, E extends Event, Meta, Context, TxCtx> extends AbstractDefaultAggregateStore<S, E, Meta, Context, TxCtx> implements AggregateStore<S, String, TxCtx> {


    public DefaultAggregateStore(EventStore<TxCtx, E, Meta, Context> eventStore, EventHandler<S, E> eventEventHandler, TransactionManager<TxCtx> transactionManager) {
        super(eventStore, eventEventHandler, transactionManager);
    }

    @Override
    protected <T, A> CompletionStage<T> fold(Publisher<A> publisher, T empty, BiFunction<T, A, T> accFunc) {
        return Flux.from(publisher)
                .reduce(empty, accFunc)
                .toFuture();
    }

}
