package fr.maif.eventsourcing.vanilla;

import fr.maif.eventsourcing.Command;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.Result;
import fr.maif.eventsourcing.State;
import fr.maif.eventsourcing.TransactionManager;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public interface EventProcessor<Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, TxCtx, Message, Meta, Context> extends Closeable {
    CompletionStage<Result<Error, ProcessingSuccess<S, E, Meta, Context, Message>>> processCommand(C command);

    CompletionStage<List<Result<Error, ProcessingSuccess<S, E, Meta, Context, Message>>>> batchProcessCommand(List<C> commands);

    CompletionStage<TransactionManager.InTransactionResult<List<Result<Error, ProcessingSuccess<S, E, Meta, Context, Message>>>>> batchProcessCommand(TxCtx ctx, List<C> commands);

    CompletionStage<Optional<S>> getAggregate(String id);

    EventStore<TxCtx, E, Meta, Context> eventStore();

    AggregateStore<S, String, TxCtx> getAggregateStore();

    default void close() throws IOException {

    };
}
