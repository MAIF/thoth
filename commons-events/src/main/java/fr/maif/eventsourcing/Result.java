package fr.maif.eventsourcing;

import io.vavr.control.Either;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public sealed interface Result<E, V> {

    record Success<E, V>(V value) implements Result<E, V> {}
    record Error<E, V>(E value) implements Result<E, V> {}

    static <E, V> Result<E, V> success(V value) {
        return new Success<>(value);
    }
    static <E, V> Result<E, V> error(E value) {
        return new Error<>(value);
    }

    static <E, V> Result<E, V> fromEither(Either<? extends E, ? extends V> either) {
        return either.fold(Result::error, Result::success);
    }

    static <E, V> Result<E, V> fromOptional(Optional<V> optional, Supplier<E> onEmpty) {
        return optional.map(v -> Result.<E, V>success(v)).orElseGet(() -> Result.<E, V>error(onEmpty.get()));
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

    default Result<E, V> onError(Consumer<E> f) {
        if (this instanceof Result.Error<E,V>(var err)) {
            f.accept(err);
        }
        return this;
    }

    default Result<E, V> onSuccess(Consumer<V> f) {
        if (this instanceof Result.Success<E,V>(var s)) {
            f.accept(s);
        }
        return this;
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
