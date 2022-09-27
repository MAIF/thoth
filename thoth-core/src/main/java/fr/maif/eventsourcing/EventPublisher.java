package fr.maif.eventsourcing;

import fr.maif.concurrent.CompletionStages;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;

import java.io.Closeable;
import java.util.concurrent.CompletionStage;

public interface EventPublisher<E extends Event, Meta, Context> extends Closeable {
    CompletionStage<Void> publish(List<EventEnvelope<E, Meta, Context>> events);

    default <TxCtx> void start(EventStore<TxCtx, E, Meta, Context> eventStore, EventStore.ConcurrentReplayStrategy concurrentReplayStrategy) {
    }
}
