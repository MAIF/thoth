package fr.maif.eventsourcing;

import io.vavr.collection.List;
import io.vavr.control.Option;

public interface EventHandler<State, Event> {

    default Option<State> deriveState(Option<State> state, List<Event> events) {
        return events.foldLeft(state, this::applyEvent);
    }

    Option<State> applyEvent(Option<State> state, Event events);
}
