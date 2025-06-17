package fr.maif.eventsourcing.vanilla;

import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import io.vavr.collection.List;

import java.io.IOException;
import java.util.concurrent.CompletionStage;

public class EventPublisherImpl<E extends Event, Meta, Context> implements EventPublisher<E, Meta, Context> {

    private final fr.maif.eventsourcing.EventPublisher<E, Meta, Context> eventPublisher;

    public EventPublisherImpl(fr.maif.eventsourcing.EventPublisher<E, Meta, Context> eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    @Override
    public CompletionStage<Void> publish(java.util.List<EventEnvelope<E, Meta, Context>> events) {
        return eventPublisher.publish(List.ofAll(events)).thenAccept(any -> {});
    }


    @Override
    public <TxCtx> CompletionStage<Void> publishNonAcknowledgedFromDb(fr.maif.eventsourcing.vanilla.EventStore<TxCtx, E, Meta, Context> eventStore, fr.maif.eventsourcing.vanilla.EventStore.ConcurrentReplayStrategy concurrentReplayStrategy) {
        return eventPublisher.publishNonAcknowledgedFromDb(eventStore.toEventStore(), EventStoreVanilla.convert(concurrentReplayStrategy))
                .thenAccept(any -> {});
    }

    @Override
    public void close() throws IOException {
        eventPublisher.close();
    }
}
