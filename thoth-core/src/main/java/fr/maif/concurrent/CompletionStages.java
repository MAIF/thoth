package fr.maif.concurrent;

import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.control.Try;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.function.Function.identity;

public class CompletionStages {

    public static <U> CompletionStage<U> completedStage(U value) {
        CompletableFuture<U> completableFuture = new CompletableFuture<>();
        completableFuture.complete(value);
        return completableFuture;
    }
    public static <U> CompletionStage<U> failedStage(Throwable e) {
        CompletableFuture<U> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(e);
        return completableFuture;
    }

    public static <T, U> CompletionStage<List<U>> traverse(List<T> elements, Function<T, CompletionStage<U>> handler) {
        return elements.foldLeft(
                completedStage(List.empty()),
                (fResult, elt) ->
                        fResult.thenCompose(listResult -> handler.apply(elt).thenApply(listResult::append))
        );
    }

    public static <T> CompletionStage<T> fromTry(Supplier<Try<T>> tryValue, Executor executor) {
        return CompletableFuture.supplyAsync(() -> tryValue.get().fold(
                CompletionStages::<T>failedStage,
                CompletionStages::completedStage
        ), executor).thenCompose(identity());
    }

    public static <T> CompletionStage<T> fromTry(Supplier<Try<T>> tryValue) {
        return CompletableFuture.supplyAsync(() -> tryValue.get().fold(
                CompletionStages::<T>failedStage,
                CompletionStages::completedStage
        )).thenCompose(identity());
    }

    public static <T> CompletionStage<T> of(Supplier<T> tryValue, Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return completedStage(tryValue.get());
            } catch (Exception e) {
                return CompletionStages.<T>failedStage(e);
            }
        }, executor).thenCompose(identity());
    }
    public static <T> CompletionStage<T> of(Supplier<T> tryValue) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return completedStage(tryValue.get());
            } catch (Exception e) {
                return CompletionStages.<T>failedStage(e);
            }
        }).thenCompose(identity());
    }

    public static <S> CompletionStage<S> successful(S value) {
        return completedStage(value);
    }
    public static <S> CompletionStage<S> failed(Throwable e) {
        return CompletionStages.failedStage(e);
    }
    public static CompletionStage<Tuple0> empty() {
        return completedStage(Tuple.empty());
    }
}
