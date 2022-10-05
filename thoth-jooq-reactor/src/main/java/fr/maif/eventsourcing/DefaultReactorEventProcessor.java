package fr.maif.eventsourcing;

import io.vavr.collection.List;
import io.vavr.control.Either;
import io.vavr.control.Option;
import reactor.core.publisher.Mono;

import java.io.IOException;

public class DefaultReactorEventProcessor<Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, TxCtx, Message, Meta, Context> implements ReactorEventProcessor<Error, S, C, E, TxCtx, Message, Meta, Context> {

    private final EventProcessor<Error, S, C, E, TxCtx, Message, Meta, Context> eventProcessor;
    private final ReactorEventStore<TxCtx, E, Meta, Context> reactorEventStore;
    private final ReactorAggregateStore<S, String, TxCtx> reactorAggregateStore;

    public DefaultReactorEventProcessor(EventProcessor<Error, S, C, E, TxCtx, Message, Meta, Context> eventProcessor) {
        this.eventProcessor = eventProcessor;
        this.reactorEventStore = ReactorEventStore.fromEventStore(eventProcessor.eventStore());
        this.reactorAggregateStore = ReactorAggregateStore.fromAggregateStore(eventProcessor.getAggregateStore());
    }

    @Override
    public Mono<Either<Error, ProcessingSuccess<S, E, Meta, Context, Message>>> processCommand(C command) {
        return Mono.fromCompletionStage(() -> eventProcessor.processCommand(command));
    }

    @Override
    public Mono<List<Either<Error, ProcessingSuccess<S, E, Meta, Context, Message>>>> batchProcessCommand(List<C> commands) {
        return Mono.fromCompletionStage(() -> eventProcessor.batchProcessCommand(commands));
    }

    @Override
    public Mono<TransactionManager.InTransactionResult<List<Either<Error, ProcessingSuccess<S, E, Meta, Context, Message>>>>> batchProcessCommand(TxCtx txCtx, List<C> commands) {
        return Mono.fromCompletionStage(() -> eventProcessor.batchProcessCommand(txCtx, commands));
    }

    @Override
    public Mono<Option<S>> getAggregate(String id) {
        return Mono.fromCompletionStage(() -> eventProcessor.getAggregate(id));
    }

    @Override
    public ReactorEventStore<TxCtx, E, Meta, Context> eventStore() {
        return reactorEventStore;
    }

    @Override
    public ReactorAggregateStore<S, String, TxCtx> getAggregateStore() {
        return reactorAggregateStore;
    }

    @Override
    public void close() throws IOException {
        eventProcessor.close();
    }
}
