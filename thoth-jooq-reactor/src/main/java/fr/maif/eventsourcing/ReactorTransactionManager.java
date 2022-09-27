package fr.maif.eventsourcing;

import fr.maif.jooq.reactor.PgAsyncPool;
import fr.maif.jooq.reactor.PgAsyncTransaction;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class ReactorTransactionManager implements TransactionManager<PgAsyncTransaction> {

    private final PgAsyncPool pgAsyncPool;

    public ReactorTransactionManager(PgAsyncPool pgAsyncPool) {
        this.pgAsyncPool = pgAsyncPool;
    }

    @Override
    public <T> CompletionStage<T> withTransaction(Function<PgAsyncTransaction, CompletionStage<T>> callBack) {
        return pgAsyncPool.inTransactionMono(tx -> Mono.fromCompletionStage(() -> callBack.apply(tx))).toFuture();
    }

}
