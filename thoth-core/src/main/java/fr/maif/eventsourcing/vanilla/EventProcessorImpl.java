package fr.maif.eventsourcing.vanilla;

import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.Result;
import fr.maif.eventsourcing.State;
import fr.maif.eventsourcing.TransactionManager;
import io.vavr.control.Either;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public class EventProcessorImpl<Error, S extends State<S>, C extends fr.maif.eventsourcing.Command<Meta, Context>, E extends Event, TxCtx, Message, Meta, Context> implements EventProcessor<Error, S, C, E, TxCtx, Message, Meta, Context> {

    private final fr.maif.eventsourcing.EventProcessor<Error, S, C, E, TxCtx, Message, Meta, Context> processor;


    public EventProcessorImpl(EventStore<TxCtx, E, Meta, Context> eventStore, TransactionManager<TxCtx> transactionManager, AggregateStore<S, String, TxCtx> aggregateStore, CommandHandler<Error, S, C, E, Message, TxCtx> commandHandler, EventHandler<S, E> eventHandler, List<Projection<TxCtx, E, Meta, Context>> projections) {
        this.processor = new fr.maif.eventsourcing.EventProcessorImpl<>(
                eventStore.toEventStore(),
                transactionManager,
                aggregateStore.toAggregateStore(),
                commandHandler.toCommandHandler(),
                eventHandler.toEventHandler(),
                io.vavr.collection.List.ofAll(projections).map(Projection::projection)
        );
    }

    public EventProcessorImpl(fr.maif.eventsourcing.EventProcessor<Error, S, C, E, TxCtx, Message, Meta, Context> processor) {
        this.processor = processor;
    }


    private static <Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, Meta, Context, Message> Result<Error, ProcessingSuccess<S, E, Meta, Context, Message>> fromPR(Either<Error, fr.maif.eventsourcing.ProcessingSuccess<S, E, Meta, Context, Message>> either) {
        return either.fold(
                err -> new Result.Error<>(err),
                success -> new Result.Success<>(ProcessingSuccess.from(success))
        );
    }

    @Override
    public CompletionStage<Result<Error, ProcessingSuccess<S, E, Meta, Context, Message>>> processCommand(C command) {
        return processor.processCommand(command).thenApply(EventProcessorImpl::fromPR);
    }

    @Override
    public CompletionStage<java.util.List<Result<Error, ProcessingSuccess<S, E, Meta, Context, Message>>>> batchProcessCommand(java.util.List<C> commands) {
        return processor.batchProcessCommand(io.vavr.collection.List.ofAll(commands))
                .thenApply(r -> r.map(either -> fromPR(either)).toJavaList());
    }

    @Override
    public CompletionStage<TransactionManager.InTransactionResult<java.util.List<Result<Error, ProcessingSuccess<S, E, Meta, Context, Message>>>>> batchProcessCommand(TxCtx txCtx, java.util.List<C> commands) {
        return processor.batchProcessCommand(txCtx, io.vavr.collection.List.ofAll(commands))
                .thenApply(result -> result.map(l -> l.map(EventProcessorImpl::fromPR).toJavaList()));
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
