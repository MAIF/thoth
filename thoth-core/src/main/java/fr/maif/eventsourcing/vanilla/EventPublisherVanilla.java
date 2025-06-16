package fr.maif.eventsourcing.vanilla;

import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventStore;
import io.vavr.collection.List;

import java.io.IOException;
import java.util.concurrent.CompletionStage;

public class EventPublisherVanilla<E extends Event, Meta, Context> implements EventPublisher<E, Meta, Context> {

    private final fr.maif.eventsourcing.EventPublisher<E, Meta, Context> eventPublisher;

    public EventPublisherVanilla(fr.maif.eventsourcing.EventPublisher<E, Meta, Context> eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    @Override
    public CompletionStage<Void> publish(java.util.List<EventEnvelope<E, Meta, Context>> events) {
        return eventPublisher.publish(List.ofAll(events)).thenAccept(any -> {});
    }

    @Override
    public <TxCtx> CompletionStage<Void> publishNonAcknowledgedFromDb(EventStore<TxCtx, E, Meta, Context> eventStore, EventStore.ConcurrentReplayStrategy concurrentReplayStrategy) {
        return eventPublisher.publishNonAcknowledgedFromDb(eventStore, concurrentReplayStrategy)
                .thenAccept(any -> {});
    }

    @Override
    public void close() throws IOException {
        eventPublisher.close();
    }
}
