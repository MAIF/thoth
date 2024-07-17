package fr.maif.eventsourcing;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Traversable;
import io.vavr.control.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static java.util.function.Function.identity;

public class DefaultReactorAggregateStore<S extends State<S>, E extends Event, Meta, Context, TxCtx> implements ReactorAggregateStore<S, String, TxCtx> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultReactorAggregateStore.class);

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
    public Mono<Map<String, Option<S>>> getAggregates(TxCtx txCtx, List<String> entityIds) {
        return this.getSnapshots(txCtx, entityIds)
                .flatMap(snapshots -> {
                    Map<String, S> indexed = snapshots.groupBy(State::entityId).mapValues(Traversable::head);
                    List<Tuple2<String, Long>> idsAndSeqNums = entityIds.map(id -> Tuple.of(id, indexed.get(id).map(s -> s.sequenceNum()).getOrElse(0L)));
                    Map<String, Option<S>> empty = HashMap.ofEntries(entityIds.map(id -> Tuple.of(id, indexed.get(id))));
                    EventStore.Query query = EventStore.Query.builder().withIdsAndSequences(idsAndSeqNums).build();
                    Flux<EventEnvelope<E, Meta, Context>> events = this.eventStore.loadEventsByQuery(txCtx, query);
                    return events.reduce(
                            empty,
                            (Map<String, Option<S>> states, EventEnvelope<E, Meta, Context> event) -> {
                                Option<S> mayBeCurrentState = states.get(event.entityId).flatMap(identity());
                                Option<S> newState = this.eventEventHandler
                                        .applyEvent(mayBeCurrentState, event.event)
                                        .map((S state) -> (S) state.withSequenceNum(event.sequenceNum));
                                LOGGER.debug("Applying {} to {} : \n -> {}", event.event, mayBeCurrentState, newState);
                                return states.put(event.entityId, newState);
                            }
                    );
                });
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
