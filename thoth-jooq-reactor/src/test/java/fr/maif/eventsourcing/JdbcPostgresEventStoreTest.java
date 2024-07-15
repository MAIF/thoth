package fr.maif.eventsourcing;

import fr.maif.jooq.PgAsyncPool;
import fr.maif.jooq.jdbc.JdbcPgAsyncPool;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultConfiguration;
import org.postgresql.ds.PGSimpleDataSource;

import java.util.concurrent.Executors;

public class JdbcPostgresEventStoreTest extends AbstractPostgresEventStoreTest {

    @Override
    protected PgAsyncPool init() {
        DefaultConfiguration jooqConfig = new DefaultConfiguration();
        jooqConfig.setSQLDialect(SQLDialect.POSTGRES);

        PGSimpleDataSource pgSimpleDataSource = new PGSimpleDataSource();
        pgSimpleDataSource.setUrl("jdbc:postgresql://"+host()+":"+port()+"/"+database());
        pgSimpleDataSource.setUser(user());
        pgSimpleDataSource.setPassword(password());

        return new JdbcPgAsyncPool(SQLDialect.POSTGRES, pgSimpleDataSource, Executors.newFixedThreadPool(3));
    }

    @Override
    String tableName() {
        return "viking_journal_1";
    }
}
