package fr.maif.eventsourcing;

public interface CommandHandlerGetter<Error, State, Command, E extends Event, Message, TxCtx> {
    CommandHandler<Error, State, Command, E, Message, TxCtx> commandHandler();
}
