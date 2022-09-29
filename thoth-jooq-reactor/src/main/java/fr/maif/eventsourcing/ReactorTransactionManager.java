package fr.maif.eventsourcing;

import fr.maif.jooq.reactor.PgAsyncPool;
import fr.maif.jooq.reactor.PgAsyncTransaction;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public interface ReactorTransactionManager<TxCtx> extends TransactionManager<TxCtx> {

    static ReactorTransactionManager<PgAsyncTransaction> create(PgAsyncPool pgAsyncPool) {
        return new ReactorTransactionManager<PgAsyncTransaction>() {
            @Override
            public <T> Mono<T> withTransactionMono(Function<PgAsyncTransaction, Mono<T>> callBack) {
                return Mono.fromCompletionStage(() -> withTransaction(tx -> callBack.apply(tx).toFuture()));
            }

            @Override
            public <T> CompletionStage<T> withTransaction(Function<PgAsyncTransaction, CompletionStage<T>> callBack) {
                return pgAsyncPool.inTransactionMono(tx -> Mono.fromCompletionStage(() -> callBack.apply(tx))).toFuture();
            }
        };
    }

    <T> Mono<T> withTransactionMono(Function<TxCtx, Mono<T>> callBack);

}
