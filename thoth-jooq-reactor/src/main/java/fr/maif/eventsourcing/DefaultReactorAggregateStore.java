package fr.maif.eventsourcing;

import io.vavr.control.Option;
import reactor.core.publisher.Mono;

public class DefaultReactorAggregateStore<S extends State<S>, E extends Event, Meta, Context, TxCtx> implements ReactorAggregateStore<S, String, TxCtx> {


    private final ReactorEventStore<TxCtx, E, Meta, Context> eventStore;
    private final EventHandler<S, E> eventEventHandler;
    private final ReactorTransactionManager<TxCtx> transactionManager;

    public DefaultReactorAggregateStore(ReactorEventStore<TxCtx, E, Meta, Context> eventStore, EventHandler<S, E> eventEventHandler, ReactorTransactionManager<TxCtx> transactionManager) {
        this.eventStore = eventStore;
        this.eventEventHandler = eventEventHandler;
        this.transactionManager = transactionManager;
    }

    @Override
    public Mono<Option<S>> getAggregate(String entityId) {
        return transactionManager.withTransaction(ctx -> getAggregate(ctx, entityId));
    }

    @Override
    public Mono<Option<S>> getAggregate(TxCtx txCtx, String entityId) {

        return this.getSnapshot(txCtx, entityId)
                .flatMap(mayBeSnapshot -> {
                    EventStore.Query query = mayBeSnapshot.fold(
                            // No snapshot defined, we read all the events
                            () -> EventStore.Query.builder().withEntityId(entityId).build(),
                            // If a snapshot is defined, we read events from seq num of the snapshot :
                            s -> EventStore.Query.builder().withSequenceFrom(s.sequenceNum()).withEntityId(entityId).build()
                    );
                    return this.eventStore.loadEventsByQuery(txCtx, query)
                            .reduce(
                                    mayBeSnapshot,
                                    (Option<S> mayBeState, EventEnvelope<E, Meta, Context> event) ->
                                            this.eventEventHandler.applyEvent(mayBeState, event.event)
                                                    .map((S state) -> (S) state.withSequenceNum(event.sequenceNum))
                            );
                });
    }
}
