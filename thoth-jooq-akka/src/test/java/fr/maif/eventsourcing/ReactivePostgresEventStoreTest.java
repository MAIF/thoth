package fr.maif.eventsourcing;

import fr.maif.jooq.PgAsyncPool;
import fr.maif.jooq.reactive.ReactivePgAsyncPool;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgBuilder;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultConfiguration;

public class ReactivePostgresEventStoreTest extends AbstractPostgresEventStoreTest {


    @Override
    protected PgAsyncPool init() {
        DefaultConfiguration jooqConfig = new DefaultConfiguration();
        jooqConfig.setSQLDialect(SQLDialect.POSTGRES);

        PoolOptions poolOptions = new PoolOptions().setMaxSize(30);
        PgConnectOptions options = new PgConnectOptions()
                .setPort(port())
                .setHost("localhost")
                .setDatabase(database())
                .setUser(user())
                .setPassword(password());
        Pool client = PgBuilder.pool()
                .using(Vertx.vertx())
                .connectingTo(options)
                .with(poolOptions)
                .build();
        return new ReactivePgAsyncPool(client, jooqConfig);
    }

    @Override
    String tableName() {
        return "viking_journal_2";
    }
}
