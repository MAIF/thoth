package fr.maif.eventsourcing;

import static org.jooq.impl.DSL.field;

import fr.maif.jooq.PgAsyncTransaction;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.Set;
import io.vavr.concurrent.Future;
import org.jooq.Field;

public class AsyncLockManager implements LockManager<PgAsyncTransaction> {
  private final static Field<String> ENTITY_ID = field("entity_id", String.class);

  private final TableNames tableNames;

  public AsyncLockManager(TableNames tableNames) {
    this.tableNames = tableNames;
  }

  @Override
  public Future<Tuple0> lock(PgAsyncTransaction transactionContext, Set<String> entityIds) {
    if(entityIds.isEmpty()) {
      return Future.successful(Tuple.empty());
    }
    return transactionContext.execute(dslContext ->
        dslContext.select().from(tableNames.lockTableName)
          .where(ENTITY_ID.in(entityIds.toJavaSet()))
          .forUpdate()
    ).map(__ -> Tuple.empty());
  }
}
