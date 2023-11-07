package fr.maif.eventsourcing.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import fr.maif.eventsourcing.*;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import io.vavr.collection.List;
import io.vavr.control.Either;
import io.vavr.control.Option;
import io.vavr.control.Try;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy.NO_STRATEGY;
import static fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy.SKIP;
import static fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy.WAIT;
import static io.vavr.API.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.jooq.impl.DSL.table;
import static org.mockito.Mockito.mock;

public class PostgresEventStoreTest {

    private PostgresEventStore<VikingEvent, Void, Void> postgresEventStore;
    private HikariDataSource dataSource;
    private DSLContext dslContext;
    private Table<Record> vikings_journal = table("vikings_journal");
    private LocalDateTime emissionDate = LocalDateTime.of(2019, 2, 1, 0, 0);
    private LocalDateTime emissionDate2 = LocalDateTime.of(2019, 2, 5, 0, 0);
    private EventEnvelope<VikingEvent, Void, Void> event1 = eventEnvelope(1L, new VikingEvent.VikingCreated("bjorn@gmail.com"), emissionDate);
    private EventEnvelope<VikingEvent, Void, Void> event2 = eventEnvelope(2L, new VikingEvent.VikingUpdated("bjorn@gmail.com"), emissionDate);
    private EventEnvelope<VikingEvent, Void, Void> event3 = eventEnvelope(3L, new VikingEvent.VikingDeleted("bjorn@gmail.com"), emissionDate);
    private EventEnvelope<VikingEvent, Void, Void> event4 = eventEnvelope(4L, new VikingEvent.VikingCreated("ragnard@gmail.com"), emissionDate2);
    private EventEnvelope<VikingEvent, Void, Void> event5 = eventEnvelope(5L, new VikingEvent.VikingUpdated("ragnard@gmail.com"), emissionDate2);
    private EventEnvelope<VikingEvent, Void, Void> event6 = eventEnvelope(6L, new VikingEvent.VikingDeleted("ragnard@gmail.com"), emissionDate2);

    @Test
    public void insertAndRead() {
        try (Connection connection = dataSource.getConnection()) {
            LocalDateTime emissionDate = LocalDateTime.now().withNano(0);
            List<EventEnvelope<VikingEvent, Void, Void>> events = List(
                    eventEnvelope(1L, new VikingEvent.VikingCreated("bjorn@gmail.com"), emissionDate),
                    eventEnvelope(2L, new VikingEvent.VikingUpdated("bjorn@gmail.com"), emissionDate),
                    eventEnvelope(3L, new VikingEvent.VikingDeleted("bjorn@gmail.com"), emissionDate)
            );
            postgresEventStore.persist(connection, events).toCompletableFuture().join();

            int count = this.dslContext.fetchCount(vikings_journal);
            assertThat(count).isEqualTo(3);


            List<EventEnvelope<VikingEvent, Void, Void>> eventEnvelopes = getFromQuery(EventStore.Query.builder().withEntityId("bjorn@gmail.com").build());

            assertThat(eventEnvelopes).isEqualTo(events);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void insertAndRollback() {
        JdbcTransactionManager jdbcTransactionManager = new JdbcTransactionManager(dataSource, Executors.newSingleThreadExecutor());

        Try.of(() ->
            jdbcTransactionManager.withTransaction( connection -> {
                LocalDateTime emissionDate = LocalDateTime.now();
                List<EventEnvelope<VikingEvent, Void, Void>> events = List(
                        eventEnvelope(1L, new VikingEvent.VikingCreated("bjorn@gmail.com"), emissionDate),
                        eventEnvelope(2L, new VikingEvent.VikingUpdated("bjorn@gmail.com"), emissionDate),
                        eventEnvelope(3L, new VikingEvent.VikingDeleted("bjorn@gmail.com"), emissionDate)
                );
                return postgresEventStore.persist(connection, events)
                        .thenApply(__ -> {
                            throw new RuntimeException();
                        });
            }).toCompletableFuture().join()
        );

        int count = this.dslContext.fetchCount(vikings_journal);
        assertThat(count).isEqualTo(0);
    }


    @Test
    public void nextSequence() {
        try (Connection connection = dataSource.getConnection()) {

            Long seq = postgresEventStore.nextSequence(connection).toCompletableFuture().join();

            assertThat(seq).isNotNull();

        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void insertDateNull() {
        try (Connection connection = dataSource.getConnection()) {
            List<EventEnvelope<VikingEvent, Void, Void>> events = List(
                    eventEnvelope(1L, new VikingEvent.VikingCreated("bjorn@gmail.com"), null)
            );
            postgresEventStore.persist(connection, events).toCompletableFuture().join();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void queryingByEntityId() {
        initDatas();
        List<EventEnvelope<VikingEvent, Void, Void>> events = getFromQuery(EventStore.Query.builder().withEntityId("bjorn@gmail.com").build());
        assertThat(events).containsExactlyInAnyOrder(event1, event2, event3);
    }

    @Test
    public void queryingBySequenceNum() {
        initDatas();
        List<EventEnvelope<VikingEvent, Void, Void>> events = getFromQuery(EventStore.Query.builder().withSequenceFrom(2L).withSequenceTo(5L).build());
        assertThat(events).containsExactlyInAnyOrder(event2, event3, event4, event5);
    }

    @Test
    public void queryingWithSize() {
        initDatas();
        List<EventEnvelope<VikingEvent, Void, Void>> events = getFromQuery(EventStore.Query.builder().withSequenceFrom(2L).withSize(2).build());
        assertThat(events).containsExactlyInAnyOrder(event2, event3);
    }

    @Test
    public void queryingByDate() {
        initDatas();
        List<EventEnvelope<VikingEvent, Void, Void>> events = getFromQuery(EventStore.Query.builder()
                .withDateFrom(LocalDateTime.of(2019, 1, 1, 0, 0))
                .withDateTo(LocalDateTime.of(2019, 2, 5, 0, 0))
                .build());
        assertThat(events).containsExactlyInAnyOrder(event1, event2, event3);
    }


    @Test
    public void queryingByPublished() {
        initDatas();
        EventEnvelope<VikingEvent, Void, Void> event1Updated = postgresEventStore.markAsPublished(event1).toCompletableFuture().join();
        List<EventEnvelope<VikingEvent, Void, Void>> events = getFromQuery(EventStore.Query.builder().withPublished(true).build());
        assertThat(events).containsExactlyInAnyOrder(event1Updated);
    }

    private Mono<Connection> transactionSource() {
        return Mono.fromCompletionStage(postgresEventStore.openTransaction());
    }


    @Test
    public void loadEventsUnpublished() {
        initDatas();
        List<EventEnvelope<VikingEvent, Void, Void>> events = List.ofAll(transactionSource()
                .flatMapMany(t -> Flux.from(postgresEventStore
                        .loadEventsUnpublished(t, NO_STRATEGY))
                        .doOnError(e -> postgresEventStore.commitOrRollback(Option.of(e), t))
                        .doOnComplete(() -> postgresEventStore.commitOrRollback(Option.none(), t))
                )
                .collectList().block()
        );

        assertThat(events).containsExactlyInAnyOrder(event1, event2, event3, event4, event5, event6);
    }

    @Test
    public void loadEventsUnpublishedSkip() throws InterruptedException {
        initDatas();
        Duration initialDelay = Duration.ofMillis(100);
        System.out.println("Running first query");
        CompletionStage<java.util.List<EventEnvelope<VikingEvent, Void, Void>>> first = transactionSource().flatMapMany(t ->
                Flux.from(postgresEventStore.loadEventsUnpublished(t, SKIP))
                        .concatMap(elt -> Flux.interval(initialDelay, initialDelay).take(1).map(__ -> elt))
                        .doOnError(e -> postgresEventStore.commitOrRollback(Option.of(e), t))
                        .doOnComplete(() -> postgresEventStore.commitOrRollback(Option.none(), t))
        ).collectList().toFuture();
        Thread.sleep(50);
        long start = System.currentTimeMillis();
        System.out.println("Running second query");
        CompletionStage<java.util.List<EventEnvelope<VikingEvent, Void, Void>>> second = transactionSource().flatMapMany(t ->
                Flux.from(postgresEventStore.loadEventsUnpublished(t, SKIP))
                        .doOnError(e -> postgresEventStore.commitOrRollback(Option.of(e), t))
                        .doOnComplete(() -> postgresEventStore.commitOrRollback(Option.none(), t))
        ).collectList().toFuture();

        List<EventEnvelope<VikingEvent, Void, Void>> events2 = List.ofAll(second.toCompletableFuture().join());
        long took = System.currentTimeMillis() - start;
        System.out.println("Second query took "+took);

        List<EventEnvelope<VikingEvent, Void, Void>> events1 = List.ofAll(first.toCompletableFuture().join());
        assertThat(events1).containsExactlyInAnyOrder(event1, event2, event3, event4, event5, event6);
        assertThat(events2).isEmpty();
        assertThat(took).isLessThan(200);
    }

    @Test
    public void loadEventsUnpublishedWait() throws InterruptedException {
        initDatas();
        CompletionStage<java.util.List<EventEnvelope<VikingEvent, Void, Void>>> first = transactionSource().flatMapMany(t ->
                Flux.from(postgresEventStore.loadEventsUnpublished(t, WAIT))
                        .concatMap(elt -> Flux.interval(Duration.ofMillis(100)).take(1).map(__ -> elt))
                        .concatMap(e -> Mono.fromCompletionStage(postgresEventStore.markAsPublished(t, e).toCompletableFuture()).map(__ -> e))
                        .doOnError(e -> postgresEventStore.commitOrRollback(Option.of(e), t))
                        .doOnComplete(() -> postgresEventStore.commitOrRollback(Option.none(), t))
        ).collectList().toFuture();
        Thread.sleep(50);
        long start = System.currentTimeMillis();
        CompletionStage<java.util.List<EventEnvelope<VikingEvent, Void, Void>>> second = transactionSource().flatMapMany(t ->
                Flux.from(postgresEventStore.loadEventsUnpublished(t, WAIT))
                        .doOnError(e -> postgresEventStore.commitOrRollback(Option.of(e), t))
                        .doOnComplete(() -> postgresEventStore.commitOrRollback(Option.none(), t))
        ).collectList().toFuture();

        List<EventEnvelope<VikingEvent, Void, Void>> events2 = List.ofAll(second.toCompletableFuture().join());
        long took = System.currentTimeMillis() - start;

        List<EventEnvelope<VikingEvent, Void, Void>> events1 = List.ofAll(first.toCompletableFuture().join());
        assertThat(events1).containsExactlyInAnyOrder(event1, event2, event3, event4, event5, event6);
        assertThat(events2).isEmpty();
        assertThat(took).isGreaterThan(600);
    }

    @Test
    public void markEventsAsPublished() throws SQLException {
        initDatas();
        List<EventEnvelope<VikingEvent, Void, Void>> events;
        try (Connection connection = dataSource.getConnection()) {
            events = List.ofAll(Flux.from(postgresEventStore.loadEventsUnpublished(connection, EventStore.ConcurrentReplayStrategy.SKIP))
                    .collectList().block()
            );
        }
        assertThat(events).containsExactlyInAnyOrder(event1, event2, event3, event4, event5, event6);
        postgresEventStore.markAsPublished(events).toCompletableFuture().join();

        List<EventEnvelope<VikingEvent, Void, Void>> published;
        try (Connection connection = dataSource.getConnection()) {
            published = List.ofAll(Flux.from(postgresEventStore.loadEventsUnpublished(connection, EventStore.ConcurrentReplayStrategy.SKIP))
                    .collectList().block());
        }
        assertThat(published).isEmpty();
    }

    private List<EventEnvelope<VikingEvent, Void, Void>> getFromQuery(EventStore.Query query) {
        return List.ofAll(Flux.from(postgresEventStore.loadEventsByQuery(query)).collectList().block());
    }

    private void initDatas() {
        try (Connection connection = dataSource.getConnection()) {

            List<EventEnvelope<VikingEvent, Void, Void>> events = List(event1, event2, event3, event4, event5, event6);
            postgresEventStore.persist(connection, events).toCompletableFuture().join();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @BeforeEach
    public void setUp() {

        ExecutorService executorService = Executors.newFixedThreadPool(20);
        EventPublisher<VikingEvent, Void, Void> eventPublisher = mock(EventPublisher.class);

        Properties props = new Properties();
        props.setProperty("dataSourceClassName", "org.postgresql.ds.PGSimpleDataSource");
        props.setProperty("dataSource.serverName", "localhost");
        props.setProperty("dataSource.portNumber", "5557");
        props.setProperty("dataSource.user", "eventsourcing");
        props.setProperty("dataSource.password", "eventsourcing");
        props.setProperty("dataSource.databaseName", "eventsourcing");
        props.setProperty("maximumPoolSize", "20");
        HikariConfig config = new HikariConfig(props);
        this.dataSource = new HikariDataSource(config);
//        this.dataSource.setJdbcUrl("jdbc:postgresql://localhost:5557/eventsourcing");
//        this.dataSource.setUsername("eventsourcing");
//        this.dataSource.setPassword("eventsourcing");
//        this.dataSource.setDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

        this.dslContext = DSL.using(dataSource, SQLDialect.POSTGRES);
        Try.of(() -> {
            this.dslContext.deleteFrom(vikings_journal).execute();
            return "";
        });

        try (Connection connection = dataSource.getConnection()) {
            executeSqlScript(connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        this.postgresEventStore = new PostgresEventStore<>(
                eventPublisher,
                dataSource,
                executorService,
                new TableNames("vikings_journal", "vikings_sequence_num"),
                jacksonEventFormat,
                JacksonSimpleFormat.empty(),
                JacksonSimpleFormat.empty()
        );
    }

    @AfterEach
    public void tearDown() {
        Try.of(() -> {
            this.dslContext.deleteFrom(vikings_journal).execute();
            return "";
        });
        dataSource.close();
    }

    private static final JacksonEventFormat<String, VikingEvent> jacksonEventFormat = new JacksonEventFormat<String, VikingEvent>() {
        ObjectMapper mapper = new ObjectMapper();
        @Override
        public Either<String, VikingEvent> read(String type, Long version, JsonNode json) {
            return Match(Tuple(type, version)).of(
                    Case(VikingEvent.VikingCreatedV1.pattern2(), (t, v) -> Either.right(mapper.convertValue(json, VikingEvent.VikingCreated.class))),
                    Case(VikingEvent.VikingUpdatedV1.pattern2(), (t, v) -> Either.right(mapper.convertValue(json, VikingEvent.VikingUpdated.class))),
                    Case(VikingEvent.VikingDeletedV1.pattern2(), (t, v) -> Either.right(mapper.convertValue(json, VikingEvent.VikingDeleted.class)))
            );
        }
        @Override
        public JsonNode write(VikingEvent json) {
            return mapper.valueToTree(json);
        }
    };

    public interface VikingEvent extends Event {

        Type<VikingEvent.VikingCreated> VikingCreatedV1 = Type.create(VikingEvent.VikingCreated.class, 1L);
        Type<VikingEvent.VikingUpdated> VikingUpdatedV1 = Type.create(VikingEvent.VikingUpdated.class, 1L);
        Type<VikingEvent.VikingDeleted> VikingDeletedV1 = Type.create(VikingEvent.VikingDeleted.class, 1L);

        class VikingCreated implements VikingEvent {
            public String name;

            public VikingCreated() {
            }

            public VikingCreated(String name) {
                this.name = name;
            }

            @Override
            public String entityId() {
                return name;
            }
            @Override
            public Type type() {
                return VikingCreatedV1;
            }

            @Override
            public String toString() {
                return new StringJoiner(", ", VikingCreated.class.getSimpleName() + "[", "]")
                        .add("name='" + name + "'")
                        .toString();
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                VikingCreated that = (VikingCreated) o;
                return Objects.equals(name, that.name);
            }

            @Override
            public int hashCode() {
                return Objects.hash(name);
            }
        }
        class VikingUpdated implements VikingEvent  {
            public String name;

            public VikingUpdated() {
            }

            public VikingUpdated(String name) {
                this.name = name;
            }

            @Override
            public String entityId() {
                return name;
            }
            @Override
            public Type type() {
                return VikingUpdatedV1;
            }

            @Override
            public String toString() {
                return new StringJoiner(", ", VikingUpdated.class.getSimpleName() + "[", "]")
                        .add("name='" + name + "'")
                        .toString();
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                VikingUpdated that = (VikingUpdated) o;
                return Objects.equals(name, that.name);
            }

            @Override
            public int hashCode() {
                return Objects.hash(name);
            }
        }
        class VikingDeleted implements VikingEvent  {
            public String name;

            public VikingDeleted() {
            }

            public VikingDeleted(String name) {
                this.name = name;
            }

            @Override
            public String entityId() {
                return name;
            }
            @Override
            public Type type() {
                return VikingDeletedV1;
            }

            @Override
            public String toString() {
                return new StringJoiner(", ", VikingDeleted.class.getSimpleName() + "[", "]")
                        .add("name='" + name + "'")
                        .toString();
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                VikingDeleted that = (VikingDeleted) o;
                return Objects.equals(name, that.name);
            }

            @Override
            public int hashCode() {
                return Objects.hash(name);
            }
        }
    }

    public static <E extends Event, Meta, Context> EventEnvelope<E, Meta, Context> eventEnvelope(Long sequenceNum, E event, LocalDateTime emissionDate) {
        return eventEnvelope(sequenceNum, event, emissionDate, false);
    }

    public static <E extends Event, Meta, Context> EventEnvelope<E, Meta, Context> eventEnvelope(Long sequenceNum, E event, LocalDateTime emissionDate, boolean published) {
        return EventEnvelope.<E, Meta, Context>builder()
                .withId(UUID.randomUUID())
                .withEntityId(event.entityId())
                .withEmissionDate(emissionDate)
                .withSequenceNum(sequenceNum)
                .withEventType(event.type().name())
                .withVersion(event.type().version())
                .withTotalMessageInTransaction(1)
                .withNumMessageInTransaction(1)
                .withTransactionId("1")
                .withEvent(event)
                .withPublished(published)
                .build();
    }

    public static void executeSqlScript(Connection conn) {
        // Delimiter
        String delimiter = ";";

        // Create scanner
        Scanner scanner;
        InputStream file = PostgresEventStoreTest.class.getClassLoader().getResourceAsStream("base.sql");
        scanner = new Scanner(file).useDelimiter(delimiter);

        // Loop through the SQL file statements
        Statement currentStatement = null;
        while(scanner.hasNext()) {
            // Get statement
            String rawStatement = scanner.next() + delimiter;
            try {
                // Execute statement
                currentStatement = conn.createStatement();
                currentStatement.execute(rawStatement);
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                // Release resources
                if (currentStatement != null) {
                    try {
                        currentStatement.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                currentStatement = null;
            }
        }
        scanner.close();
    }

}