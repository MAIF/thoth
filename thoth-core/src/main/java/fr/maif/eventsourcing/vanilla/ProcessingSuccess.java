package fr.maif.eventsourcing.vanilla;

import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.State;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

public class ProcessingSuccess<S extends State<S>, E extends Event, Meta, Context, Message> {

    public final Optional<S> previousState;
    public final Optional<S> currentState;
    public final List<EventEnvelope<E, Meta, Context>> events;
    public final Message message;

    public ProcessingSuccess(Optional<S> previousState, Optional<S> currentState, List<EventEnvelope<E, Meta, Context>> events, Message message) {
        this.previousState = previousState;
        this.currentState = currentState;
        this.events = events;
        this.message = message;
    }

    public static <S extends State<S>, E extends Event, Meta, Context, Message> ProcessingSuccess<S, E, Meta, Context, Message> from(fr.maif.eventsourcing.ProcessingSuccess<S, E, Meta, Context, Message> processingSuccess) {
        return new ProcessingSuccess<>(
                processingSuccess.previousState.toJavaOptional(),
                processingSuccess.currentState.toJavaOptional(),
                processingSuccess.events.toJavaList(),
                processingSuccess.message
        );
    }

    public Optional<S> getPreviousState() {
        return previousState;
    }

    public Optional<S> getCurrentState() {
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
        return new StringJoiner(", ", fr.maif.eventsourcing.ProcessingSuccess.class.getSimpleName() + "[", "]")
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
        fr.maif.eventsourcing.ProcessingSuccess<?, ?, ?, ?, ?> that = (fr.maif.eventsourcing.ProcessingSuccess<?, ?, ?, ?, ?>) o;
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
