package fr.maif.eventsourcing.vanilla;

import fr.maif.eventsourcing.*;
import io.vavr.control.Either;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

public class EventProcessorVanilla<Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, TxCtx, Message, Meta, Context> implements EventProcessor<Error, S, C, E, TxCtx, Message, Meta, Context> {

    private final fr.maif.eventsourcing.EventProcessor<Error, S, C, E, TxCtx, Message, Meta, Context> processor;

    public EventProcessorVanilla(fr.maif.eventsourcing.EventProcessor<Error, S, C, E, TxCtx, Message, Meta, Context> processor) {
        this.processor = processor;
    }

    private static <E, A> Result<E, A> fromEither(Either<E, A> either) {
        return either.fold(
                err -> new Result.Error<>(err),
                success -> new Result.Success<>(success)
        );
    }

    @Override
    public CompletionStage<Result<Error, ProcessingSuccess<S, E, Meta, Context, Message>>> processCommand(C command) {
        return processor.processCommand(command).thenApply(EventProcessorVanilla::fromEither);
    }

    @Override
    public CompletionStage<java.util.List<Result<Error, ProcessingSuccess<S, E, Meta, Context, Message>>>> batchProcessCommand(java.util.List<C> commands) {
        return processor.batchProcessCommand(io.vavr.collection.List.ofAll(commands)).thenApply(r -> r.map(EventProcessorVanilla::fromEither).toJavaList());
    }

    @Override
    public CompletionStage<TransactionManager.InTransactionResult<java.util.List<Result<Error, ProcessingSuccess<S, E, Meta, Context, Message>>>>> batchProcessCommand(TxCtx txCtx, java.util.List<C> commands) {
        return processor.batchProcessCommand(txCtx, io.vavr.collection.List.ofAll(commands))
                .thenApply(result -> result.map(l -> l.map(EventProcessorVanilla::fromEither).toJavaList()));
    }

    @Override
    public CompletionStage<Optional<S>> getAggregate(String id) {
        return processor.getAggregate(id).thenApply(opt -> opt.toJavaOptional());
    }

    @Override
    public fr.maif.eventsourcing.vanilla.EventStore<TxCtx, E, Meta, Context> eventStore() {
        return new EventStoreVanilla<>(processor.eventStore());
    }

    @Override
    public fr.maif.eventsourcing.vanilla.AggregateStore<S, String, TxCtx> getAggregateStore() {
        return new AggregateStoreVanilla(processor.getAggregateStore());
    }
}
