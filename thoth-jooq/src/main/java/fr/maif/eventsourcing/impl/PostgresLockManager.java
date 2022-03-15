package fr.maif.eventsourcing.impl;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

import io.vavr.collection.HashSet;
import java.sql.Connection;

import java.util.Collections;
import java.util.Objects;
import java.util.stream.Collectors;
import org.jooq.Field;
import org.jooq.InsertValuesStepN;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import fr.maif.eventsourcing.LockManager;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.Set;
import io.vavr.concurrent.Future;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresLockManager implements LockManager<Connection> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresLockManager.class);
    private final static Field<String> ENTITY_ID = field("entity_id", String.class);

    private final TableNames tableNames;

    public PostgresLockManager(TableNames tableNames) {
        if(Objects.isNull(tableNames.lockTableName)) {
            throw new IllegalArgumentException("A PostgresLockManager is defined but lockTableName is null");
        }
        this.tableNames = tableNames;
    }

    @Override
    public Future<Tuple0> lock(Connection connection, Set<String> entityIds) {
        if(entityIds.isEmpty()) {
            return Future.successful(Tuple.empty());
        }

        return Future.of(() ->{
            final Set<String> retrievedIds = HashSet.ofAll(DSL.using(connection).select(ENTITY_ID)
                .from(tableNames.lockTableName).where(ENTITY_ID.in(entityIds.toJavaSet()))
                .forUpdate()
                .fetch()
                .stream()
                .map(Record1::component1)
                .collect(Collectors.toSet()));

            if(retrievedIds.size() < entityIds.size()) {
                final Set<String> missings = entityIds.diff(retrievedIds);
                missings.foldLeft(
                    DSL.using(connection)
                        .insertInto(table(tableNames.lockTableName),
                            Collections.singletonList(ENTITY_ID)),
                    InsertValuesStepN::values
                ).onConflictDoNothing()
                .execute();
                
            }
            return Tuple.empty();
        });
    }
}
