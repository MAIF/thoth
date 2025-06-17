package fr.maif.eventsourcing;

import java.util.function.Supplier;

public class Lazy<T> {
    private final io.vavr.Lazy<T> inner;

    private Lazy(io.vavr.Lazy<T> inner) {
        this.inner = inner;
    }

    public static <T> Lazy<T> of(Supplier<T> supplier) {
        return new Lazy<>(io.vavr.Lazy.of(supplier));
    }

    public io.vavr.Lazy<T> toVavr() {
        return inner;
    }
}
