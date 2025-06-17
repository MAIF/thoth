package fr.maif.eventsourcing.vanilla.format;

import io.vavr.control.Option;

import java.util.Optional;
import java.util.function.Function;

public interface SimpleFormat<E, Format> {

    Optional<E> read(Optional<Format> json);

    Optional<Format> write(Optional<E> json);

    static <E, Format> SimpleFormat<E, Format> of(Function<Format, Optional<E>> des, Function<E, Optional<Format>> ser) {
        return new SimpleFormat<E, Format>() {
            @Override
            public Optional<E> read(Optional<Format> json) {
                return json.flatMap(des);
            }

            @Override
            public Optional<Format> write(Optional<E> json) {
                return json.flatMap(ser);
            }
        };
    }

    static <E, Format> EmptyFormat<E, Format> empty() {
        return new EmptyFormat<>();
    }

    class EmptyFormat<E, Format> implements SimpleFormat<E, Format> {
        @Override
        public Optional<E> read(Optional<Format> json) {
            return Optional.empty();
        }

        @Override
        public Optional<Format> write(Optional<E> json) {
            return Optional.empty();
        }
    }

    default fr.maif.eventsourcing.format.SimpleFormat<E, Format> toFormat() {
        var _this = this;
        return new fr.maif.eventsourcing.format.SimpleFormat<>() {
            @Override
            public Option<E> read(Option<Format> json) {
                return Option.ofOptional(_this.read(json.toJavaOptional()));
            }

            @Override
            public Option<Format> write(Option<E> json) {
                return Option.ofOptional(_this.write(json.toJavaOptional()));
            }
        };
    }
}
