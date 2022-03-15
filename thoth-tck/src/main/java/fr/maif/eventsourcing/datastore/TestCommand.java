package fr.maif.eventsourcing.datastore;

import fr.maif.eventsourcing.Command;
import io.vavr.API;
import io.vavr.Lazy;
import io.vavr.Tuple0;

public abstract class TestCommand implements Command<Tuple0, Tuple0> {
    public final String id;

    public static API.Match.Pattern0<SimpleCommand> $SimpleCommand() {
        return API.Match.Pattern0.of(SimpleCommand.class);
    }

    public static API.Match.Pattern0<MultiEventCommand> $MultiEventCommand() {
        return API.Match.Pattern0.of(MultiEventCommand.class);
    }

    public static API.Match.Pattern0<DeleteCommand> $DeleteCommand() {
        return API.Match.Pattern0.of(DeleteCommand.class);
    }

    public static API.Match.Pattern0<NonConcurrentCommand> $NonConcurrentCommand() {
        return API.Match.Pattern0.of(NonConcurrentCommand.class);
    }

    public TestCommand(String id) {
        this.id = id;
    }

    public static class SimpleCommand extends TestCommand {
        public SimpleCommand(String id) {
            super(id);
        }
    }

    public static class MultiEventCommand extends TestCommand {
        public MultiEventCommand(String id) {
            super(id);
        }
    }

    public static class InvalidCommand extends TestCommand {
        public InvalidCommand(String id) {
            super(id);
        }
    }

    public static class DeleteCommand extends TestCommand {
        public DeleteCommand(String id) {
            super(id);
        }
    }

    public static class NonConcurrentCommand extends TestCommand {
        public NonConcurrentCommand(String id) {
            super(id);
        }

        @Override
        public boolean concurrent() {
            return false;
        }
    }

    @Override
    public Lazy<String> entityId() {
        return Lazy.of(() -> id);
    }
}
