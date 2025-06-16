package fr.maif.eventsourcing.vanilla;

import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventStore;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletionStage;

public interface EventPublisher<E extends Event, Meta, Context> extends Closeable {
    CompletionStage<Void> publish(List<EventEnvelope<E, Meta, Context>> events);

    <TxCtx> CompletionStage<Void> publishNonAcknowledgedFromDb(fr.maif.eventsourcing.EventStore<TxCtx, E, Meta, Context> eventStore, fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy concurrentReplayStrategy);

    default <TxCtx> void start(fr.maif.eventsourcing.EventStore<TxCtx, E, Meta, Context> eventStore, EventStore.ConcurrentReplayStrategy concurrentReplayStrategy) {}
}
