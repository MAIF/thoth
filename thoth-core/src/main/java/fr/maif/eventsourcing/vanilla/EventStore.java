package fr.maif.eventsourcing.vanilla;

import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy;
import fr.maif.eventsourcing.EventStore.Query;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public interface EventStore<TxCtx, E extends Event, Meta, Context> {

    CompletionStage<Void> persist(TxCtx transactionContext, List<EventEnvelope<E, Meta, Context>> events);

    CompletionStage<Long> lastPublishedSequence();

    Publisher<EventEnvelope<E, Meta, Context>> loadEventsUnpublished(TxCtx tx, ConcurrentReplayStrategy concurrentReplayStrategy);

    Publisher<EventEnvelope<E, Meta, Context>> loadEventsByQuery(TxCtx tx, Query query);

    Publisher<EventEnvelope<E, Meta, Context>> loadEventsByQuery(Query query);

    default Publisher<EventEnvelope<E, Meta, Context>> loadEvents(String id) {
        return loadEventsByQuery(Query.builder().withEntityId(id).build());
    }

    default Publisher<EventEnvelope<E, Meta, Context>> loadAllEvents() {
        return loadEventsByQuery(Query.builder().build());
    }

    CompletionStage<Long> nextSequence(TxCtx tx);

    CompletionStage<List<Long>> nextSequences(TxCtx tx, Integer count);

    CompletionStage<Void> publish(List<EventEnvelope<E, Meta, Context>> events);

    CompletionStage<EventEnvelope<E, Meta, Context>> markAsPublished(TxCtx tx, EventEnvelope<E, Meta, Context> eventEnvelope);

    CompletionStage<List<EventEnvelope<E, Meta, Context>>> markAsPublished(TxCtx tx, List<EventEnvelope<E, Meta, Context>> eventEnvelopes);

    CompletionStage<EventEnvelope<E, Meta, Context>> markAsPublished(EventEnvelope<E, Meta, Context> eventEnvelope);

    CompletionStage<List<EventEnvelope<E, Meta, Context>>> markAsPublished(List<EventEnvelope<E, Meta, Context>> eventEnvelopes);

    CompletionStage<TxCtx> openTransaction();

    CompletionStage<Void> commitOrRollback(Optional<Throwable> of, TxCtx tx);

    EventPublisher<E, Meta, Context> eventPublisher();

}
