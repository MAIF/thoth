package fr.maif.eventsourcing;

import fr.maif.jooq.PgAsyncPool;
import fr.maif.jooq.PgAsyncTransaction;
import io.vavr.concurrent.Future;

import java.util.function.Function;

public class ReactiveTransactionManager implements TransactionManager<PgAsyncTransaction> {

    private final PgAsyncPool pgAsyncPool;

    public ReactiveTransactionManager(PgAsyncPool pgAsyncPool) {
        this.pgAsyncPool = pgAsyncPool;
    }

    @Override
    public <T> Future<T> withTransaction(Function<PgAsyncTransaction, Future<T>> callBack) {
        return pgAsyncPool.inTransaction(callBack);
    }
}
