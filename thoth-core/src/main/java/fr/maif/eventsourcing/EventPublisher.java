package fr.maif.eventsourcing;

import io.vavr.Tuple0;
import io.vavr.collection.List;

import java.io.Closeable;
import java.util.concurrent.CompletionStage;

public interface EventPublisher<E extends Event, Meta, Context> extends Closeable {
    CompletionStage<Tuple0> publish(List<EventEnvelope<E, Meta, Context>> events);

    <TxCtx> CompletionStage<Tuple0> publishNonAcknowledgedFromDb(EventStore<TxCtx, E, Meta, Context> eventStore, EventStore.ConcurrentReplayStrategy concurrentReplayStrategy);

    default <TxCtx> void start(EventStore<TxCtx, E, Meta, Context> eventStore, EventStore.ConcurrentReplayStrategy concurrentReplayStrategy) {}
}
