package fr.maif.akka.eventsourcing;

import akka.actor.ActorSystem;
import akka.stream.Materializer;
import akka.stream.javadsl.Source;
import fr.maif.eventsourcing.*;
import fr.maif.eventsourcing.impl.AbstractDefaultAggregateStore;
import org.reactivestreams.Publisher;

import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

public class DefaultAggregateStore<S extends State<S>, E extends Event, Meta, Context, TxCtx> extends AbstractDefaultAggregateStore<S, E, Meta, Context, TxCtx> implements AggregateStore<S, String, TxCtx> {

    private final Materializer materializer;

    public DefaultAggregateStore(EventStore<TxCtx, E, Meta, Context> eventStore, EventHandler<S, E> eventEventHandler, ActorSystem system, TransactionManager<TxCtx> transactionManager) {
        this(eventStore, eventEventHandler, Materializer.createMaterializer(system), transactionManager);
    }

    public DefaultAggregateStore(EventStore<TxCtx, E, Meta, Context> eventStore, EventHandler<S, E> eventEventHandler, Materializer materializer, TransactionManager<TxCtx> transactionManager) {
        super(eventStore, eventEventHandler, transactionManager, shouldLockEntityForUpdate);
        this.materializer = materializer;
    }

    @Override
    protected <T, A> CompletionStage<T> fold(Publisher<A> publisher, T empty, BiFunction<T, A, T> accFunc) {
        return Source.fromPublisher(publisher)
                .runFold(empty, accFunc::apply, materializer);
    }

}
