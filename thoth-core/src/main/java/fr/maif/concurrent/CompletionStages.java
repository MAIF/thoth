package fr.maif.concurrent;

import io.vavr.collection.List;
import io.vavr.control.Try;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.function.Function.identity;

public class CompletionStages {

    public static <T, U> CompletionStage<List<U>> traverse(List<T> elements, Function<T, CompletionStage<U>> handler) {
        return elements.foldLeft(
                CompletableFuture.completedStage(List.empty()),
                (fResult, elt) ->
                        fResult.thenCompose(listResult -> handler.apply(elt).thenApply(listResult::append))
        );
    }

    public static <T> CompletionStage<T> fromTry(Supplier<Try<T>> tryValue, Executor executor) {
        return CompletableFuture.supplyAsync(() -> tryValue.get().fold(
                CompletableFuture::<T>failedStage,
                CompletableFuture::completedStage
        ), executor).thenCompose(identity());
    }

    public static <T> CompletionStage<T> fromTry(Supplier<Try<T>> tryValue) {
        return CompletableFuture.supplyAsync(() -> tryValue.get().fold(
                CompletableFuture::<T>failedStage,
                CompletableFuture::completedStage
        )).thenCompose(identity());
    }

    public static <T> CompletionStage<T> of(Supplier<T> tryValue, Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return CompletableFuture.completedStage(tryValue.get());
            } catch (Exception e) {
                return CompletableFuture.<T>failedStage(e);
            }
        }, executor).thenCompose(identity());
    }
    public static <T> CompletionStage<T> of(Supplier<T> tryValue) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return CompletableFuture.completedStage(tryValue.get());
            } catch (Exception e) {
                return CompletableFuture.<T>failedStage(e);
            }
        }).thenCompose(identity());
    }

    public static <S> CompletionStage<S> successful(S value) {
        return CompletableFuture.completedStage(value);
    }
    public static <S> CompletionStage<S> failed(Throwable e) {
        return CompletableFuture.failedStage(e);
    }
    public static CompletionStage<Void> empty() {
        return CompletableFuture.runAsync(() -> {});
    }
}
