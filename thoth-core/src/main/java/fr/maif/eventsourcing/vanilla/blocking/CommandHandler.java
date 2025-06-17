package fr.maif.eventsourcing.vanilla.blocking;

import fr.maif.concurrent.CompletionStages;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.Events;
import fr.maif.eventsourcing.Result;
import fr.maif.eventsourcing.Unit;
import io.vavr.control.Either;
import io.vavr.control.Option;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

public interface CommandHandler<Error, State, Command, E extends Event, Message, TxCtx> {

    /**
     *
     * @param ctx transaction context
     * @param state current state
     * @param command the command to handle
     * @return An error or events
     */
    Result<Error, Events<E, Message>> handleCommand(TxCtx ctx, Optional<State> state, Command command);


    default Result<Error, Events<E, Unit>> events(E... events) {
        return Result.success(new Events<>(io.vavr.collection.List.of(events), Unit.unit()));
    }

    default Result<Error, Events<E, Unit>> events(List<E> events) {
        return Result.success(new Events<>(io.vavr.collection.List.ofAll(events), Unit.unit()));
    }

    default Result<Error, Events<E, Message>> events(Message message, E... events) {
        return Result.success(Events.events(message, io.vavr.collection.List.of(events)));
    }

    default Result<Error, Events<E, Message>> events(Message message, List<E> events) {
        return Result.success(Events.events(message, io.vavr.collection.List.ofAll(events)));
    }

    default Result<Error, Events<E, Message>> fail(Error error) {
        return Result.error(error);
    }

    default CompletionStage<Result<Error, Events<E, Message>>> failAsync(Error error) {
        return CompletionStages.completedStage(Result.error(error));
    }


    default fr.maif.eventsourcing.CommandHandler<Error, State, Command, E, Message, TxCtx> toCommandHandler(Executor executor) {
        var _this = this;
        return (tx, state, command) ->
                CompletableFuture.supplyAsync(() -> switch (_this.handleCommand(tx, state.toJavaOptional(), command)){
                    case Result.Success(var s) -> Either.right(s);
                    case Result.Error(var e) -> Either.left(e);
                }, executor);
    }
}
