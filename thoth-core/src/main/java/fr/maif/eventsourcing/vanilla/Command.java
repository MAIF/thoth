package fr.maif.eventsourcing.vanilla;

import fr.maif.eventsourcing.Lazy;
import io.vavr.control.Option;

import java.util.Optional;

public interface Command<Meta, Context> extends fr.maif.eventsourcing.Command<Meta, Context> {

    default Optional<Context> getContext() {
        return Optional.empty();
    }

    default Optional<Meta> getMetadata() {
        return Optional.empty();
    }

    default Optional<String> getSystemId() {
        return Optional.empty();
    }

    default Optional<String> getUserId() {
        return Optional.empty();
    }

    Lazy<String> getEntityId();

    default io.vavr.Lazy<String> entityId() {
        return getEntityId().toVavr();
    }

    default Option<Context> context() {
        return Option.ofOptional(getContext());
    }

    default Option<Meta> metadata() {
        return Option.ofOptional(getMetadata());
    }

    default Option<String> systemId() {
        return Option.ofOptional(getSystemId());
    }

    default Option<String> userId() {
        return Option.ofOptional(getUserId());
    }
}
