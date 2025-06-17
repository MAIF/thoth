package fr.maif.eventsourcing.vanilla;

import fr.maif.concurrent.CompletionStages;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.Events;
import fr.maif.eventsourcing.Result;
import fr.maif.eventsourcing.Unit;
import io.vavr.collection.List;
import io.vavr.control.Either;
import io.vavr.control.Option;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 *
 * The command is the interface that need to be implemented in order to handle command.
 *
 * @param <Error> the type of the error channel
 * @param <State> the type of the state
 * @param <Command> the type of the command
 * @param <E> the type of the event
 * @param <Message> the type message
 * @param <TxCtx> the type of the transaction context e.g the connection in a jdbc context
 */
public interface CommandHandler<Error, State, Command, E extends Event, Message, TxCtx> {

    /**
     *
     * @param ctx
     * @param state
     * @param command
     * @return
     */
    CompletionStage<Result<Error, Events<E, Message>>> handleCommand(TxCtx ctx, Optional<State> state, Command command);

    default CompletionStage<Result<Error, Events<E, Unit>>> eventsAsync(E... events) {
        return CompletionStages.completedStage(Result.success(new Events<>(List.of(events), Unit.unit())));
    }

    default CompletionStage<Result<Error, Events<E, Message>>> eventsAsync(Message message, E... events) {
        return CompletionStages.completedStage(Result.success(Events.events(message, List.of(events))));
    }

    default Result<Error, Events<E, Unit>> events(E... events) {
        return Result.success(new Events<>(List.of(events), Unit.unit()));
    }

    default Result<Error, Events<E, Message>> events(Message message, E... events) {
        return Result.success(Events.events(message, List.of(events)));
    }

    default Result<Error, Events<E, Message>> fail(Error error) {
        return Result.error(error);
    }

    default CompletionStage<Result<Error, Events<E, Message>>> failAsync(Error error) {
        return CompletionStages.completedStage(Result.error(error));
    }

    default fr.maif.eventsourcing.CommandHandler<Error, State, Command, E, Message, TxCtx> toCommandHandler() {
        var _this = this;
        return (txCtx, state, command) -> _this.handleCommand(txCtx, state.toJavaOptional(), command).thenApply(r -> switch (r){
            case Result.Success(var s) -> Either.right(s);
            case Result.Error(var s) -> Either.left(s);
        });
    }
}
