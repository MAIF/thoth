package fr.maif.eventsourcing.format;

import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.control.Option;

import java.util.function.Function;

public interface JacksonSimpleFormat<E> extends SimpleFormat<E, JsonNode> {

    JacksonJsonFormat jacksonJsonFormat = new JacksonJsonFormat();

    static <E> JacksonSimpleFormat<E> of(Function<JsonNode, Option<E>> des, Function<E, Option<JsonNode>> ser) {
        return new JacksonSimpleFormat<E>() {
            @Override
            public Option<E> read(Option<JsonNode> json) {
                return json.flatMap(des);
            }

            @Override
            public Option<JsonNode> write(Option<E> json) {
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
        public Option<JsonNode> read(Option<JsonNode> json) {
            return json;
        }

        @Override
        public Option<JsonNode> write(Option<JsonNode> json) {
            return json;
        }
    }
    class JacksonEmptyFormat<E> implements JacksonSimpleFormat<E> {
        @Override
        public Option<E> read(Option<JsonNode> json) {
            return Option.none();
        }

        @Override
        public Option<JsonNode> write(Option<E> json) {
            return Option.none();
        }
    }

}
