package fr.maif.eventsourcing.vanilla;

import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.Unit;

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

public class Events<E extends Event, Message> {

    public final List<E> events;
    public final Message message;

    public Events(List<E> events, Message message) {
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
        return new Events<>(List.of(), null);
    }

    public static <E extends Event> Events<E, Unit> events(E... events) {
        return new Events<>(List.of(events), Unit.unit());
    }
    public static <E extends Event> Events<E, Unit> events(List<E> events) {
        return new Events<>(events, Unit.unit());
    }
    public static <E extends Event, M> Events<E, M> events(M message, E... events) {
        return new Events<>(List.of(events), message);
    }
    public static <E extends Event, M> Events<E, M> events(M message, List<E> events) {
        return new Events<>(events, message);
    }
}
