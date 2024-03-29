package fr.maif.eventsourcing;

import io.vavr.collection.List;
import io.vavr.control.Either;
import io.vavr.control.Option;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletionStage;

public interface EventProcessor<Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, TxCtx, Message, Meta, Context> extends Closeable {
    CompletionStage<Either<Error, ProcessingSuccess<S, E, Meta, Context, Message>>> processCommand(C command);

    CompletionStage<List<Either<Error, ProcessingSuccess<S, E, Meta, Context, Message>>>> batchProcessCommand(List<C> commands);

    CompletionStage<TransactionManager.InTransactionResult<List<Either<Error, ProcessingSuccess<S, E, Meta, Context, Message>>>>> batchProcessCommand(TxCtx ctx, List<C> commands);

    CompletionStage<Option<S>> getAggregate(String id);

    EventStore<TxCtx, E, Meta, Context> eventStore();

    AggregateStore<S, String, TxCtx> getAggregateStore();

    default void close() throws IOException {

    };
}
