package fr.maif.eventsourcing;

import io.vavr.collection.List;
import io.vavr.control.Option;

import java.util.Objects;
import java.util.StringJoiner;

public class ProcessingSuccess<S extends State<S>, E extends Event, Meta, Context, Message> {

    public final Option<S> previousState;
    public final Option<S> currentState;
    public final List<EventEnvelope<E, Meta, Context>> events;
    public final Message message;

    public ProcessingSuccess(Option<S> previousState, Option<S> currentState, List<EventEnvelope<E, Meta, Context>> events, Message message) {
        this.previousState = previousState;
        this.currentState = currentState;
        this.events = events;
        this.message = message;
    }

    public Option<S> getPreviousState() {
        return previousState;
    }

    public Option<S> getCurrentState() {
        return currentState;
    }

    public List<EventEnvelope<E, Meta, Context>> getEvents() {
        return events;
    }

    public Message getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ProcessingSuccess.class.getSimpleName() + "[", "]")
                .add("previousState=" + previousState)
                .add("currentState=" + currentState)
                .add("events=" + events)
                .add("message=" + message)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProcessingSuccess<?, ?, ?, ?, ?> that = (ProcessingSuccess<?, ?, ?, ?, ?>) o;
        return Objects.equals(previousState, that.previousState) &&
                Objects.equals(currentState, that.currentState) &&
                Objects.equals(events, that.events) &&
                Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(previousState, currentState, events, message);
    }
}
