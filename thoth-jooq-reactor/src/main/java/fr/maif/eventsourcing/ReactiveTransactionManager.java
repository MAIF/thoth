package fr.maif.eventsourcing;

import fr.maif.jooq.PgAsyncPool;
import fr.maif.jooq.PgAsyncTransaction;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class ReactiveTransactionManager implements TransactionManager<PgAsyncTransaction> {

    private final PgAsyncPool pgAsyncPool;

    public ReactiveTransactionManager(PgAsyncPool pgAsyncPool) {
        this.pgAsyncPool = pgAsyncPool;
    }

    @Override
    public <T> CompletionStage<T> withTransaction(Function<PgAsyncTransaction, CompletionStage<T>> callBack) {
        return pgAsyncPool.inTransaction(callBack);
    }
}
