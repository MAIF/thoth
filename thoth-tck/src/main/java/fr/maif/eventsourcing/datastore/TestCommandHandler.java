package fr.maif.eventsourcing.datastore;

import fr.maif.eventsourcing.CommandHandler;
import fr.maif.eventsourcing.Events;
import io.vavr.Tuple0;
import io.vavr.concurrent.Future;
import io.vavr.control.Either;
import io.vavr.control.Option;

import java.util.function.Function;

import static io.vavr.API.Case;
import static io.vavr.API.Match;

public class TestCommandHandler<TxCtx> implements CommandHandler<String, TestState, TestCommand, TestEvent, Tuple0, TxCtx> {
    @Override
    public Future<Either<String, Events<TestEvent, Tuple0>>> handleCommand(
            TxCtx useless,
            Option<TestState> previousState,
            TestCommand command) {
        return Future.of(() -> Match(command).option(
                Case(TestCommand.$SimpleCommand(), cmd -> events(new TestEvent.SimpleEvent(cmd.id))),
                Case(TestCommand.$MultiEventCommand(), cmd -> events(new TestEvent.SimpleEvent(cmd.id), new TestEvent.SimpleEvent(cmd.id))),
                Case(TestCommand.$DeleteCommand(), cmd -> events(new TestEvent.DeleteEvent(cmd.id)))
        ).toEither(() -> "Unknown command").flatMap(Function.identity()));
    }
}
