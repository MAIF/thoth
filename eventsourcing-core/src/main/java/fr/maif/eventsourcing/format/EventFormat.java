package fr.maif.eventsourcing.format;

import fr.maif.eventsourcing.Type;
import io.vavr.control.Either;

public interface EventFormat<Err, E, Format> {

    Either<Err, E> read(String type, Long version, Format json);

    default Either<Err, E> read(Type<E> type, Format json) {
        return read(type.name(), type.version(), json);
    }

    Format write(E json);
}
