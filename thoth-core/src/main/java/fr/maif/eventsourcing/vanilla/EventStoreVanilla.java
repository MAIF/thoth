package fr.maif.eventsourcing.vanilla;

import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import io.vavr.Tuple;
import io.vavr.collection.List;
import io.vavr.control.Option;
import org.reactivestreams.Publisher;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

public class EventStoreVanilla<TxCtx, E extends Event, Meta, Context> implements EventStore<TxCtx, E, Meta, Context> {
    
    private final fr.maif.eventsourcing.EventStore<TxCtx, E, Meta, Context> eventStore;

    public EventStoreVanilla(fr.maif.eventsourcing.EventStore<TxCtx, E, Meta, Context> eventStore) {
        this.eventStore = eventStore;
    }

    @Override
    public CompletionStage<Void> persist(TxCtx transactionContext, java.util.List<EventEnvelope<E, Meta, Context>> events) {
        return eventStore.persist(transactionContext, List.ofAll(events)).thenAccept(any -> {});
    }

    @Override
    public CompletionStage<Long> lastPublishedSequence() {
        return eventStore.lastPublishedSequence();
    }

    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadEventsUnpublished(TxCtx tx, EventStore.ConcurrentReplayStrategy concurrentReplayStrategy) {
        return eventStore.loadEventsUnpublished(tx, convert(concurrentReplayStrategy));
    }

    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadEventsByQuery(TxCtx tx, EventStore.Query query) {
        return eventStore.loadEventsByQuery(tx, convert(query));
    }

    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadEventsByQuery(EventStore.Query query) {
        return eventStore.loadEventsByQuery(convert(query));
    }

    @Override
    public CompletionStage<Long> nextSequence(TxCtx tx) {
        return eventStore.nextSequence(tx);
    }

    @Override
    public CompletionStage<java.util.List<Long>> nextSequences(TxCtx tx, Integer count) {
        return eventStore.nextSequences(tx, count).thenApply(r -> r.toJavaList());
    }

    @Override
    public CompletionStage<Void> publish(java.util.List<EventEnvelope<E, Meta, Context>> events) {
        return eventStore.publish(List.ofAll(events)).thenAccept(any -> {});
    }

    @Override
    public CompletionStage<EventEnvelope<E, Meta, Context>> markAsPublished(TxCtx tx, EventEnvelope<E, Meta, Context> eventEnvelope) {
        return eventStore.markAsPublished(tx, eventEnvelope);
    }

    @Override
    public CompletionStage<java.util.List<EventEnvelope<E, Meta, Context>>> markAsPublished(TxCtx tx, java.util.List<EventEnvelope<E, Meta, Context>> eventEnvelopes) {
        return eventStore.markAsPublished(tx, List.ofAll(eventEnvelopes)).thenApply(r -> r.toJavaList());
    }

    @Override
    public CompletionStage<EventEnvelope<E, Meta, Context>> markAsPublished(EventEnvelope<E, Meta, Context> eventEnvelope) {
        return eventStore.markAsPublished(eventEnvelope);
    }

    @Override
    public CompletionStage<java.util.List<EventEnvelope<E, Meta, Context>>> markAsPublished(java.util.List<EventEnvelope<E, Meta, Context>> eventEnvelopes) {
        return eventStore.markAsPublished(List.ofAll(eventEnvelopes)).thenApply(r -> r.toJavaList());
    }

    @Override
    public CompletionStage<TxCtx> openTransaction() {
        return eventStore.openTransaction();
    }

    @Override
    public CompletionStage<Void> commitOrRollback(Optional<Throwable> of, TxCtx tx) {
        return eventStore.commitOrRollback(Option.ofOptional(of), tx).thenAccept(any -> {});
    }

    @Override
    public fr.maif.eventsourcing.vanilla.EventPublisher<E, Meta, Context> eventPublisher() {
        return new EventPublisherVanilla<>(eventStore.eventPublisher());
    }

    private static fr.maif.eventsourcing.EventStore.Query convert(EventStore.Query query) {
        return fr.maif.eventsourcing.EventStore.Query.builder()
                .withDateFrom(query.dateFrom)
                .withDateTo(query.dateTo)
                .withEntityId(query.entityId)
                .withSize(query.size)
                .withUserId(query.userId)
                .withSystemId(query.systemId)
                .withPublished(query.published)
                .withSequenceFrom(query.sequenceFrom)
                .withSequenceTo(query.sequenceTo)
                .withIdsAndSequences(List.ofAll(query.idsAndSequences).map(idAndSequence -> Tuple.of(idAndSequence.id(), idAndSequence.sequence())))
                .build();
    }

    private static fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy convert(EventStore.ConcurrentReplayStrategy strategy) {
        return switch (strategy) {
            case SKIP -> fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy.SKIP;
            case WAIT -> fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy.WAIT;
            case NO_STRATEGY -> fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy.NO_STRATEGY;
        };
    }
}
