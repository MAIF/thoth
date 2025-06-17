package fr.maif.eventsourcing.vanilla.format;

import fr.maif.eventsourcing.Result;
import fr.maif.eventsourcing.Type;
import io.vavr.control.Either;

public interface EventFormat<Err, E, Format> {

    Result<Err, E> read(String type, Long version, Format json);

    default Result<Err, E> read(Type<E> type, Format json) {
        return read(type.name(), type.version(), json);
    }

    Format write(E json);

    default fr.maif.eventsourcing.format.EventFormat<Err, E, Format> toFormat() {
        var _this = this;
        return new fr.maif.eventsourcing.format.EventFormat<Err, E, Format>() {
            @Override
            public Format write(E json) {
                return _this.write(json);
            }

            @Override
            public Either<Err, E> read(Type<E> type, Format json) {
                return _this.read(type, json).fold(Either::left, Either::right);
            }

            @Override
            public Either<Err, E> read(String type, Long version, Format json) {
                return _this.read(type, version,  json).fold(Either::left, Either::right);
            }
        };
    }
}
