package fr.maif.eventsourcing;

import io.vavr.collection.List;
import io.vavr.control.Either;
import io.vavr.control.Option;
import reactor.core.publisher.Mono;

import java.io.Closeable;
import java.io.IOException;

public interface ReactorEventProcessor<Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, TxCtx, Message, Meta, Context> extends Closeable {

    static <Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, TxCtx, Message, Meta, Context> ReactorEventProcessor<Error, S, C, E, TxCtx, Message, Meta, Context> create(EventProcessor<Error, S, C, E, TxCtx, Message, Meta, Context> eventProcessor) {
        return new DefaultReactorEventProcessor<>(eventProcessor);
    }

    Mono<Either<Error, ProcessingSuccess<S, E, Meta, Context, Message>>> processCommand(C command);

    Mono<List<Either<Error, ProcessingSuccess<S, E, Meta, Context, Message>>>> batchProcessCommand(List<C> commands);

    Mono<TransactionManager.InTransactionResult<List<Either<Error, ProcessingSuccess<S, E, Meta, Context, Message>>>>> batchProcessCommand(TxCtx ctx, List<C> commands);

    Mono<Option<S>> getAggregate(String id);

    ReactorEventStore<TxCtx, E, Meta, Context> eventStore();

    ReactorAggregateStore<S, String, TxCtx> getAggregateStore();

}
