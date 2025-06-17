package fr.maif.eventsourcing.vanilla.format;

import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.control.Either;

public interface JacksonEventFormat<Err, E> extends EventFormat<Err, E, JsonNode> {

    default fr.maif.eventsourcing.format.JacksonEventFormat<Err, E> toFormat() {
        var _this = this;
        return new fr.maif.eventsourcing.format.JacksonEventFormat<Err, E>() {
            @Override
            public Either<Err, E> read(String type, Long version, JsonNode json) {
                return _this.read(type, version, json).fold(Either::left, Either::right);
            }

            @Override
            public JsonNode write(E json) {
                return _this.write(json);
            }
        };
    }
}
