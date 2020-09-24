package fr.maif.eventsourcing;

import io.vavr.Lazy;
import io.vavr.control.Option;

public interface Command<Meta, Context> {

    default Boolean hasId() {
        return true;
    }

    Lazy<String> entityId();

    default Option<Context> context() {
        return Option.none();
    }

    default Option<Meta> metadata() {
        return Option.none();
    }

    default Option<String> systemId() {
        return Option.none();
    }

    default Option<String> userId() {
        return Option.none();
    }
}
