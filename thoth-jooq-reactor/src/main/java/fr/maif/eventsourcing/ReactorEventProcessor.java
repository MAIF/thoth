package fr.maif.eventsourcing;

import io.vavr.collection.List;
import io.vavr.control.Either;
import io.vavr.control.Option;
import reactor.core.publisher.Mono;

public interface ReactorEventProcessor<Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, TxCtx, Message, Meta, Context> {

    static <Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, TxCtx, Message, Meta, Context> ReactorEventProcessor<Error, S, C, E, TxCtx, Message, Meta, Context> create(EventProcessor<Error, S, C, E, TxCtx, Message, Meta, Context> eventProcessor) {
        return new ReactorEventProcessor<Error, S, C, E, TxCtx, Message, Meta, Context>() {
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
                return ReactorEventStore.fromEventStore(eventProcessor.eventStore());
            }

            @Override
            public ReactorAggregateStore<S, String, TxCtx> getAggregateStore() {
                return ReactorAggregateStore.fromAggregateStore(eventProcessor.getAggregateStore());
            }

            @Override
            public void close() {
                eventProcessor.close();
            }
        };
    }

    Mono<Either<Error, ProcessingSuccess<S, E, Meta, Context, Message>>> processCommand(C command);

    Mono<List<Either<Error, ProcessingSuccess<S, E, Meta, Context, Message>>>> batchProcessCommand(List<C> commands);

    Mono<TransactionManager.InTransactionResult<List<Either<Error, ProcessingSuccess<S, E, Meta, Context, Message>>>>> batchProcessCommand(TxCtx ctx, List<C> commands);

    Mono<Option<S>> getAggregate(String id);

    ReactorEventStore<TxCtx, E, Meta, Context> eventStore();

    ReactorAggregateStore<S, String, TxCtx> getAggregateStore();

    void close();
}
