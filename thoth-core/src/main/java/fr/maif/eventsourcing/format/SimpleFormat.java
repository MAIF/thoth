package fr.maif.eventsourcing.format;

import io.vavr.control.Option;

import java.util.function.Function;

public interface SimpleFormat<E, Format> {

    Option<E> read(Option<Format> json);

    Option<Format> write(Option<E> json);

    static <E, Format> SimpleFormat<E, Format> of(Function<Format, Option<E>> des, Function<E, Option<Format>> ser) {
        return new SimpleFormat<E, Format>() {
            @Override
            public Option<E> read(Option<Format> json) {
                return json.flatMap(des);
            }

            @Override
            public Option<Format> write(Option<E> json) {
                return json.flatMap(ser);
            }
        };
    }

    static <E, Format> EmptyFormat<E, Format> empty() {
        return new EmptyFormat<>();
    }

    class EmptyFormat<E, Format> implements SimpleFormat<E, Format> {
        @Override
        public Option<E> read(Option<Format> json) {
            return Option.none();
        }

        @Override
        public Option<Format> write(Option<E> json) {
            return Option.none();
        }
    }
}
