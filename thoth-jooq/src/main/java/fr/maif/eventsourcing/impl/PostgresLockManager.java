package fr.maif.eventsourcing.impl;

import static org.jooq.impl.DSL.field;

import java.sql.Connection;

import org.jooq.Field;
import org.jooq.impl.DSL;

import fr.maif.eventsourcing.LockManager;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.Set;
import io.vavr.concurrent.Future;

public class PostgresLockManager implements LockManager<Connection> {
    private final static Field<String> ENTITY_ID = field("entity_id", String.class);

    private final TableNames tableNames;

    public PostgresLockManager(TableNames tableNames) {
        this.tableNames = tableNames;
    }

    @Override
    public Future<Tuple0> lock(Connection connection, Set<String> entityIds) {
        if(entityIds.isEmpty()) {
            return Future.successful(Tuple.empty());
        }

        return Future.of(() ->{
            DSL.using(connection).select().from(tableNames.lockTableName).where(ENTITY_ID.in(entityIds.toJavaSet())).forUpdate().fetch();
            return Tuple.empty();
        });
    }
}
