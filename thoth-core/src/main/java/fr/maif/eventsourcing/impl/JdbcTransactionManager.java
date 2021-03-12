package fr.maif.eventsourcing.impl;

import fr.maif.akka.AkkaExecutionContext;
import fr.maif.eventsourcing.TransactionManager;
import io.vavr.concurrent.Future;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

public class JdbcTransactionManager implements TransactionManager<Connection> {

    private final Logger LOGGER = LoggerFactory.getLogger(JdbcTransactionManager.class);
    private final DataSource dataSource;
    private final ExecutorService executor;

    public JdbcTransactionManager(DataSource dataSource, ExecutorService executor) {
        this.dataSource = dataSource;
        this.executor = executor;
    }

    @Override
    public <T> Future<T> withTransaction(Function<Connection, Future<T>> callBack) {

        return Future.of(executor, this.dataSource::getConnection)
                .flatMap(connection ->
                        Future.of(executor, () -> {
                            connection.setAutoCommit(false);
                            return connection;
                        })
                        .flatMap(callBack)
                        .flatMap(r -> commit(connection).map(__ -> r))
                        .recoverWith(e -> {
                            LOGGER.error("Error, rollbacking, {}", e);
                            return rollback(connection).flatMap(__ -> Future.failed(e));
                        })
                        .flatMap(r -> closeConnection(connection).map(__ -> r))
                        .recoverWith(e -> {
                            LOGGER.error("Error, closing connection, {}", e);
                            return closeConnection(connection).flatMap(__ -> Future.failed(e));
                        })
                );
    }

    private Future<Boolean> rollback(Connection connection) {
        return Future.of(executor, () -> {

                connection.rollback();
                return true;
            });
    }

    private Future<Boolean> commit(Connection connection) {
        return Future.fromTry(executor, Try.of(() -> {
            connection.commit();
            return true;
        }));
    }

    private Future<Boolean> closeConnection(Connection connection) {
        return Future.fromTry(executor, Try.of(() -> {
                connection.close();
                return true;
        }));
    }
}
