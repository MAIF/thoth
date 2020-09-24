package fr.maif.eventsourcing;

import io.vavr.API;
import io.vavr.Tuple2;

import static io.vavr.API.$;
import static io.vavr.Patterns.$Tuple2;
import static io.vavr.Predicates.instanceOf;

public interface Type<T> {

    Class<T> clazz();
    String name();
    Long version();
    API.Match.Pattern0<? extends T> pattern();
    API.Match.Pattern2<Tuple2<String, Long>, String, Long> pattern2();


    static <T> Type<T> create(Class<T> clazz, Long version) {
        return create(clazz, clazz.getSimpleName(), version);
    }
    static <T> Type<T> create(Class<T> clazz, String name, Long version) {
        return new Type<T>() {
            @Override
            public Class<T> clazz() {
                return clazz;
            }

            @Override
            public String name() {
                return name;
            }

            @Override
            public Long version() {
                return version;
            }

            @Override
            public API.Match.Pattern0<? extends T> pattern() {
                return $(instanceOf(clazz));
            }

            @Override
            public API.Match.Pattern2<Tuple2<String, Long>, String, Long> pattern2() {
                return $Tuple2($(name), $(version));
            }
        };
    }
}

