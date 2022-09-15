package fr.maif.eventsourcing;

import io.vavr.Tuple0;
import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;
import io.vavr.control.Either;
import io.vavr.control.Option;

import java.util.concurrent.CompletableFuture;
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
    CompletionStage<Either<Error, Events<E, Message>>> handleCommand(TxCtx ctx, Option<State> state, Command command);

    default CompletionStage<Either<Error, Events<E, Tuple0>>> eventsAsync(E... events) {
        return CompletableFuture.completedStage(Either.right(Events.events(List.of(events))));
    }

    default CompletionStage<Either<Error, Events<E, Message>>> eventsAsync(Message message, E... events) {
        return CompletableFuture.completedStage(Either.right(Events.events(message, List.of(events))));
    }

    default Either<Error, Events<E, Tuple0>> events(E... events) {
        return Either.right(Events.events(List.of(events)));
    }

    default Either<Error, Events<E, Message>> events(Message message, E... events) {
        return Either.right(Events.events(message, List.of(events)));
    }

    default Either<Error, Events<E, Message>> fail(Error error) {
        return Either.left(error);
    }

    default CompletionStage<Either<Error, Events<E, Message>>> failAsync(Error error) {
        return CompletableFuture.completedStage(Either.left(error));
    }

}
