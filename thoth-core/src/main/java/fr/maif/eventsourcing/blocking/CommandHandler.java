package fr.maif.eventsourcing.blocking;

import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.Events;
import io.vavr.control.Either;
import io.vavr.control.Option;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public interface CommandHandler<Error, State, Command, E extends Event, Message, TxCtx> {

    /**
     *
     * @param ctx transaction context
     * @param state current state
     * @param command the command to handle
     * @return An error or events
     */
    Either<Error, Events<E, Message>> handleCommand(TxCtx ctx, Option<State> state, Command command);

    default fr.maif.eventsourcing.CommandHandler<Error, State, Command, E, Message, TxCtx> toCommandHandler(Executor executor) {
        var _this = this;
        return (tx, state, command) ->
                CompletableFuture.supplyAsync(() -> _this.handleCommand(tx, state, command), executor);
    }
}
