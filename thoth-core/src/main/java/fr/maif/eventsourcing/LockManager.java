package fr.maif.eventsourcing;

import io.vavr.Tuple0;
import io.vavr.collection.Set;
import io.vavr.concurrent.Future;

public interface LockManager<TxCtx> {
    Future<Tuple0> lock(TxCtx transactionContext, Set<String> entityIds);
}
