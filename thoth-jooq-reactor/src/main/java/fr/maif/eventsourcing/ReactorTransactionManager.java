package fr.maif.eventsourcing;

import fr.maif.jooq.reactor.PgAsyncPool;
import fr.maif.jooq.reactor.PgAsyncTransaction;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public interface ReactorTransactionManager<TxCtx> {

    static ReactorTransactionManager<PgAsyncTransaction> create(PgAsyncPool pgAsyncPool) {
        return new DefaultReactorTransactionManager(pgAsyncPool);
    }

    <T> Mono<T> withTransaction(Function<TxCtx, Mono<T>> callBack);

    default TransactionManager<TxCtx> toTransactionManager() {
        var _this = this;
        return new TransactionManager<TxCtx>() {
            @Override
            public <T> CompletionStage<T> withTransaction(Function<TxCtx, CompletionStage<T>> callBack) {
                return _this.withTransaction(tx -> Mono.fromCompletionStage(() -> callBack.apply(tx))).toFuture();
            }
        };
    }
}
