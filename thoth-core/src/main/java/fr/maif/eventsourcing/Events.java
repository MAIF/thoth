package fr.maif.eventsourcing;

import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.collection.Seq;

import java.util.Objects;
import java.util.StringJoiner;

public class Events<E extends Event, Message> {

    public final Seq<E> events;
    public final Message message;

    public Events(Seq<E> events, Message message) {
        this.events = events;
        this.message = message;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Events.class.getSimpleName() + "[", "]")
                .add("events=" + events)
                .add("message=" + message)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Events<?, ?> events1 = (Events<?, ?>) o;
        return Objects.equals(events, events1.events) &&
                Objects.equals(message, events1.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(events, message);
    }

    public static <E extends Event, Message> Events<E, Message> empty() {
        return new Events<>(List.empty(), null);
    }

    public static <E extends Event> Events<E, Tuple0> events(E... events) {
        return new Events<>(List.of(events), Tuple.empty());
    }
    public static <E extends Event> Events<E, Tuple0> events(Seq<E> events) {
        return new Events<>(events, Tuple.empty());
    }
    public static <E extends Event, M> Events<E, M> events(M message, E... events) {
        return new Events<>(List.of(events), message);
    }
    public static <E extends Event, M> Events<E, M> events(M message, Seq<E> events) {
        return new Events<>(events, message);
    }
}
