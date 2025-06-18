package fr.maif.eventsourcing.vanilla.blocking;

import fr.maif.concurrent.CompletionStages;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.vanilla.Events;
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
        return Result.success(new Events<>(List.of(events), Unit.unit()));
    }

    default Result<Error, Events<E, Unit>> events(List<E> events) {
        return Result.success(new Events<>(events, Unit.unit()));
    }

    default Result<Error, Events<E, Message>> events(Message message, E... events) {
        return Result.success(Events.events(message, List.of(events)));
    }

    default Result<Error, Events<E, Message>> events(Message message, List<E> events) {
        return Result.success(Events.events(message, events));
    }

    default Result<Error, Events<E, Message>> fail(Error error) {
        return Result.error(error);
    }

    default CompletionStage<Result<Error, Events<E, Message>>> failAsync(Error error) {
        return CompletionStages.completedStage(Result.error(error));
    }


    default fr.maif.eventsourcing.vanilla.CommandHandler<Error, State, Command, E, Message, TxCtx> toCommandHandler(Executor executor) {
        var _this = this;
        return new fr.maif.eventsourcing.vanilla.CommandHandler<>() {
            @Override
            public CompletionStage<Result<Error, fr.maif.eventsourcing.vanilla.Events<E, Message>>> handleCommand(TxCtx txCtx, Optional<State> state, Command command) {
                return CompletableFuture.supplyAsync(() -> {
                            Result<Error, Events<E, Message>> errorEventsResult = _this.handleCommand(txCtx, state, command);
                            return errorEventsResult.map(events -> new fr.maif.eventsourcing.vanilla.Events<>(events.events, events.message));
                        }
                        , executor);
            }

            @Override
            public fr.maif.eventsourcing.CommandHandler<Error, State, Command, E, Message, TxCtx> commandHandler() {
                return new fr.maif.eventsourcing.CommandHandler<>() {
                    @Override
                    public CompletionStage<Either<Error, fr.maif.eventsourcing.Events<E, Message>>> handleCommand(TxCtx txCtx, Option<State> state, Command command) {
                        return CompletableFuture.supplyAsync(() -> switch (_this.handleCommand(txCtx, state.toJavaOptional(), command)){
                            case Result.Success(var s) -> Either.right(new fr.maif.eventsourcing.Events<>(io.vavr.collection.List.ofAll(s.events), s.message));
                            case Result.Error(var e) -> Either.left(e);
                        }, executor);
                    }
                };
            }
        };
    }
}
