package fr.maif.eventsourcing;

import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;

import java.io.Closeable;

public interface EventPublisher<E extends Event, Meta, Context> extends Closeable {
    Future<Tuple0> publish(List<EventEnvelope<E, Meta, Context>> events);
}
