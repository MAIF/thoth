package fr.maif.eventsourcing.vanilla;

import java.util.Optional;

public interface EventHandler<State, Event> {

    Optional<State> applyEvent(Optional<State> state, Event events);
}
