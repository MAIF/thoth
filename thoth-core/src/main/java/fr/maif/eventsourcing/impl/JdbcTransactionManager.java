package fr.maif.eventsourcing.impl;

import fr.maif.concurrent.CompletionStages;
import fr.maif.eventsourcing.TransactionManager;
import io.vavr.concurrent.Future;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

public class JdbcTransactionManager implements TransactionManager<Connection> {

    private final Logger LOGGER = LoggerFactory.getLogger(JdbcTransactionManager.class);
    private final DataSource dataSource;
    private final Executor executor;

    public JdbcTransactionManager(DataSource dataSource, Executor executor) {
        this.dataSource = dataSource;
        this.executor = executor;
    }

    @Override
    public <T> CompletionStage<T> withTransaction(Function<Connection, CompletionStage<T>> callBack) {

        return CompletableFuture.supplyAsync(() -> {
                    try {
                        return this.dataSource.getConnection();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }, executor)
                .thenCompose(connection -> CompletableFuture
                        .supplyAsync(() -> {
                            try {
                                connection.setAutoCommit(false);
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                            return connection;
                        }, executor)
                        .thenCompose(callBack)
                        .thenCompose(r -> commit(connection).thenApply(__ -> r))
                        .exceptionallyCompose(e -> {
                            LOGGER.error("Error, rollbacking, {}", e);
                            return rollback(connection).thenCompose(__ -> CompletionStages.<T>failedStage(e));
                        })
                        .thenCompose(r -> closeConnection(connection).thenApply(__ -> r))
                        .exceptionallyCompose(e -> {
                            LOGGER.error("Error, closing connection, {}", e);
                            return closeConnection(connection).thenCompose(__ -> CompletionStages.<T>failedStage(e));
                        })
                );
    }

    private CompletionStage<Boolean> rollback(Connection connection) {
        return CompletionStages.fromTry(() -> Try.of(() -> {
                connection.rollback();
                return true;
            }), executor);
    }

    private CompletionStage<Boolean> commit(Connection connection) {
        return CompletionStages.fromTry(() -> Try.of(() -> {
            connection.commit();
            return true;
        }), executor);
    }

    private CompletionStage<Boolean> closeConnection(Connection connection) {
        return CompletionStages.fromTry(() -> Try.of(() -> {
            connection.close();
            return true;
        }), executor);
    }
}
