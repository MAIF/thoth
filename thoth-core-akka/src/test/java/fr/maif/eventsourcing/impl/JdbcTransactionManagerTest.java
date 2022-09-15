package fr.maif.eventsourcing.impl;

import akka.actor.ActorSystem;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.testkit.javadsl.TestKit;
import fr.maif.concurrent.CompletionStages;
import fr.maif.jdbc.Convertions;
import fr.maif.jdbc.DbUtils;
import fr.maif.jdbc.Sql;
import io.vavr.collection.HashMap;
import io.vavr.concurrent.Future;
import io.vavr.control.Try;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

public class JdbcTransactionManagerTest implements DbUtils {

    private static ActorSystem system;

    @Test
    public void test_commit() throws SQLException, ExecutionException, InterruptedException {
        createDatabase(1);
        ExecutorService ec = Executors.newFixedThreadPool(5);
        JdbcTransactionManager jdbcTransactionManager = new JdbcTransactionManager(dataSource, ec);

        jdbcTransactionManager.withTransaction(connection ->
                        Sql.of(connection, system)
                                .update("update bands1 set name = ? where band_id = ?").params("The mars volta", 2)
                                .closeConnection(false)
                                .count()
                                .runWith(Sink.head(), Materializer.createMaterializer(system))
                                .toCompletableFuture()
        ).toCompletableFuture().join();


        List<Map<String, String>> results = Sql.of(dataSource.getConnection(), system)
                .select("select * from bands1").as(Convertions.map)
                .get()
                .runWith(Sink.seq(), Materializer.createMaterializer(system))
                .toCompletableFuture().get();

        assertThat(results).contains(
                HashMap.of("BAND_ID", "1", "CREATEDAT", "2012-09-17 18:47:52.1", "NAME", "Envy" ).toJavaMap(),
                HashMap.of("BAND_ID", "2", "CREATEDAT", "2012-09-17 18:47:52.1", "NAME", "The mars volta").toJavaMap(),
                HashMap.of("BAND_ID", "3", "CREATEDAT", "2012-09-17 18:47:52.1", "NAME", "Explosion in the sky").toJavaMap()
        );

    }

    @Test
    public void test_rollback() throws SQLException, ExecutionException, InterruptedException {
        createDatabase(2);
        ExecutorService ec = Executors.newFixedThreadPool(5);
        JdbcTransactionManager jdbcTransactionManager = new JdbcTransactionManager(dataSource, ec);

        Try.of(() -> {
            jdbcTransactionManager.withTransaction(connection ->
                            Sql.of(connection, system)
                                    .update("update bands2 set name = ? where band_id = ?").params("The mars volta", 2)
                                    .closeConnection(false)
                                    .count()
                                    .runWith(Sink.head(), Materializer.createMaterializer(system))
                                    .toCompletableFuture()
                            .thenCompose(__ -> CompletionStages.failed(new RuntimeException("Oups")))
            ).toCompletableFuture().join();
            return true;
        });

        List<Map<String, String>> results = Sql.of(dataSource.getConnection(), system)
                .select("select * from bands2").as(Convertions.map)
                .get()
                .runWith(Sink.seq(), Materializer.createMaterializer(system))
                .toCompletableFuture().get();

        assertThat(results).contains(
                HashMap.of("BAND_ID", "1", "CREATEDAT", "2012-09-17 18:47:52.1", "NAME", "Envy" ).toJavaMap(),
                HashMap.of("BAND_ID", "2", "CREATEDAT", "2012-09-17 18:47:52.1", "NAME", "At the drive in").toJavaMap(),
                HashMap.of("BAND_ID", "3", "CREATEDAT", "2012-09-17 18:47:52.1", "NAME", "Explosion in the sky").toJavaMap()
        );

    }

    @BeforeEach
    @AfterEach
    public void cleanUp() {
        dropAll(1);
        dropAll(2);
    }


    @BeforeAll
    public static void setup() {
        system = ActorSystem.create();
    }

    @AfterAll
    public static void teardown() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }
}