package fr.maif.eventsourcing;

import java.util.function.Function;

public sealed interface Result<E, V> {

    record Success<E, V>(V value) implements Result<E, V> {}
    record Error<E, V>(E value) implements Result<E, V> {}

    static <E, V> Success<E, V> success(V value) {
        return new Success<>(value);
    }
    static <E, V> Error<E, V> error(E value) {
        return new Error<>(value);
    }

    default V get() {
        return switch (this) {
            case Success(var value) -> value;
            case Error(var err) -> throw new RuntimeException(err.toString());
        };
    }

    default <U> Result<E, U> map(Function<V, U> f) {
        return switch (this) {
            case Success(var value) -> new Success<E, U>(f.apply(value));
            case Error(var err) -> new Error<E, U>(err);
        };
    }
    default <E1> Result<E1, V> mapError(Function<E, E1> f) {
        return switch (this) {
            case Success(var value) -> new Success<E1, V>(value);
            case Error(var err) -> new Error<E1, V>(f.apply(err));
        };
    }

    default <U> Result<E, U> flatMap(Function<V, Result<E, U>> f) {
        return switch (this) {
            case Success(var value) -> f.apply(value);
            case Error(var err) -> new Error<E, U>(err);
        };
    }

    default <U> U fold(Function<E, U> onError, Function<V, U> onSuccess) {
        return switch (this) {
            case Success(var value) -> onSuccess.apply(value);
            case Error(var err) -> onError.apply(err);
        };
    }

}
