package fr.maif.jdbc;

import akka.actor.ActorSystem;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.testkit.javadsl.TestKit;
import io.vavr.collection.HashMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by adelegue on 12/10/2016.
 */
public class SqlTest implements DbUtils {

    private static ActorSystem system;

    @Test
    public void testSelect() throws SQLException, ExecutionException, InterruptedException, ParseException {
        createDatabase(21);
        Connection connection = dataSource.getConnection();
        List<Map<String, String>> results = Sql.of(connection, system)
                .select("select * from bands21").as(Convertions.map)
                .get()
                .runWith(Sink.seq(), Materializer.createMaterializer(system))
                .toCompletableFuture().get();

        assertThat(results).contains(
            HashMap.of("BAND_ID", "1", "CREATEDAT", "2012-09-17 18:47:52.1", "NAME", "Envy" ).toJavaMap(),
            HashMap.of("BAND_ID", "2", "CREATEDAT", "2012-09-17 18:47:52.1", "NAME", "At the drive in").toJavaMap(),
            HashMap.of("BAND_ID", "3", "CREATEDAT", "2012-09-17 18:47:52.1", "NAME", "Explosion in the sky").toJavaMap()
        );

        assertThat(connection.isClosed()).isTrue();
    }

    @Test
    public void testUpdate() throws SQLException, ExecutionException, InterruptedException, ParseException {
        createDatabase(22);
        Connection connection = dataSource.getConnection();
        Integer updates = Sql.of(connection, system)
                .update("update bands22 set name = ? where band_id = ?").params("The mars volta", 2)
                .closeConnection(false)
                .count()
                .runWith(Sink.head(), Materializer.createMaterializer(system))
                .toCompletableFuture().get();

        assertThat(updates).isEqualTo(1);
        assertThat(connection.isClosed()).isFalse();

        List<Map<String, String>> results = Sql.of(connection, system)
                .select("select * from bands22").as(Convertions.map)
                .get()
                .runWith(Sink.seq(), Materializer.createMaterializer(system))
                .toCompletableFuture().get();

        assertThat(results).contains(
                HashMap.of("BAND_ID", "1", "CREATEDAT", "2012-09-17 18:47:52.1", "NAME", "Envy" ).toJavaMap(),
                HashMap.of("BAND_ID", "2", "CREATEDAT", "2012-09-17 18:47:52.1", "NAME", "The mars volta").toJavaMap(),
                HashMap.of("BAND_ID", "3", "CREATEDAT", "2012-09-17 18:47:52.1", "NAME", "Explosion in the sky").toJavaMap()
        );
        assertThat(connection.isClosed()).isTrue();
    }


    @BeforeEach
    @AfterEach
    public void cleanUp() {
        dropAll(21);
        dropAll(22);
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