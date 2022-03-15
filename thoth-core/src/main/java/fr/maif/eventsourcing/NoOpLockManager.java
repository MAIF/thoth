package fr.maif.eventsourcing;

import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.Set;
import io.vavr.concurrent.Future;

public class NoOpLockManager<TxCtx> implements LockManager<TxCtx>{
    @Override
    public Future<Tuple0> lock(TxCtx transactionManager, Set<String> entityIds) {
        return Future.successful(Tuple.empty());
    }

    @Override
    public boolean isNoOp() {
        return true;
    }
}
