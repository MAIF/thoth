package fr.maif.eventsourcing.vanilla.format;

import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.control.Option;

import java.util.Optional;
import java.util.function.Function;

public interface JacksonSimpleFormat<E> extends SimpleFormat<E, JsonNode> {

    JacksonJsonFormat jacksonJsonFormat = new JacksonJsonFormat();

    static <E> JacksonSimpleFormat<E> of(Function<JsonNode, Optional<E>> des, Function<E, Optional<JsonNode>> ser) {
        return new JacksonSimpleFormat<E>() {
            @Override
            public Optional<E> read(Optional<JsonNode> json) {
                return json.flatMap(des);
            }

            @Override
            public Optional<JsonNode> write(Optional<E> json) {
                return json.flatMap(ser);
            }
        };
    }

    static JacksonJsonFormat json() {
        return jacksonJsonFormat;
    }

    static <E> JacksonSimpleFormat<E> empty() {
        return new JacksonEmptyFormat<>();
    }

    class JacksonJsonFormat implements JacksonSimpleFormat<JsonNode> {
        @Override
        public Optional<JsonNode> read(Optional<JsonNode> json) {
            return json;
        }

        @Override
        public Optional<JsonNode> write(Optional<JsonNode> json) {
            return json;
        }
    }
    class JacksonEmptyFormat<E> implements JacksonSimpleFormat<E> {
        @Override
        public Optional<E> read(Optional<JsonNode> json) {
            return Optional.empty();
        }

        @Override
        public Optional<JsonNode> write(Optional<E> json) {
            return Optional.empty();
        }
    }

    default fr.maif.eventsourcing.format.JacksonSimpleFormat<E> toFormat() {
        var _this = this;
        return new fr.maif.eventsourcing.format.JacksonSimpleFormat<>() {
            @Override
            public Option<E> read(Option<JsonNode> json) {
                return Option.ofOptional(_this.read(json.toJavaOptional()));
            }

            @Override
            public Option<JsonNode> write(Option<E> json) {
                return Option.ofOptional(_this.write(json.toJavaOptional()));
            }
        };
    }

}
