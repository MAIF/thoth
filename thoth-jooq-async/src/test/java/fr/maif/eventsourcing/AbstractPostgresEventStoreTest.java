package fr.maif.eventsourcing;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.jooq.PgAsyncPool;
import fr.maif.jooq.PgAsyncTransaction;
import io.vavr.API;
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
import org.postgresql.ds.PGSimpleDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletionStage;

import static fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy.SKIP;
import static fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy.WAIT;
import static io.vavr.API.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.impl.DSL.table;
import static org.mockito.Mockito.mock;

public abstract class AbstractPostgresEventStoreTest {

    protected Integer port = 5557;
    protected String host = "localhost";
    protected String database = "eventsourcing";
    protected String user = "eventsourcing";
    protected String password = "eventsourcing";

    private ActorSystem system;
    private ReactivePostgresEventStore<VikingEvent, Void, Void> postgresEventStore;
    private PgAsyncPool pgAsyncPool;
    private DSLContext dslContext;
    private Table<Record> vikings_journal = table(tableName());

    private LocalDateTime emissionDate = LocalDateTime.of(2019, 2, 1, 0, 0);
    private LocalDateTime emissionDate2 = LocalDateTime.of(2019, 2, 5, 0, 0);
    private EventEnvelope<VikingEvent, Void, Void> event1 = eventEnvelope(1L, new VikingEvent.VikingCreated("bjorn@gmail.com"), emissionDate);
    private EventEnvelope<VikingEvent, Void, Void> event2 = eventEnvelope(2L, new VikingEvent.VikingUpdated("bjorn@gmail.com"), emissionDate);
    private EventEnvelope<VikingEvent, Void, Void> event3 = eventEnvelope(3L, new VikingEvent.VikingDeleted("bjorn@gmail.com"), emissionDate);
    private EventEnvelope<VikingEvent, Void, Void> event4 = eventEnvelope(4L, new VikingEvent.VikingCreated("ragnard@gmail.com"), emissionDate2);
    private EventEnvelope<VikingEvent, Void, Void> event5 = eventEnvelope(5L, new VikingEvent.VikingUpdated("ragnard@gmail.com"), emissionDate2);
    private EventEnvelope<VikingEvent, Void, Void> event6 = eventEnvelope(6L, new VikingEvent.VikingDeleted("ragnard@gmail.com"), emissionDate2);
    private ReactiveTransactionManager reactiveTransactionManager;

    private static Date date(int year, int month, int day) {
        return new Date(year, month, day);
    }

    abstract String tableName();

    @Test
    public void insertAndRead() {
        LocalDateTime emissionDate = LocalDateTime.now().withNano(0);
        List<EventEnvelope<VikingEvent, Void, Void>> events = API.List(
                eventEnvelope(1L, new VikingEvent.VikingCreated("bjorn@gmail.com"), emissionDate),
                eventEnvelope(2L, new VikingEvent.VikingUpdated("bjorn@gmail.com"), emissionDate),
                eventEnvelope(3L, new VikingEvent.VikingDeleted("bjorn@gmail.com"), emissionDate)
        );
        pgAsyncPool.inTransaction(ctx ->
                        postgresEventStore.persist(ctx, events)
                )
                .get();

        int count = this.dslContext.fetchCount(vikings_journal);
        assertThat(count).isEqualTo(3);


        List<EventEnvelope<VikingEvent, Void, Void>> eventEnvelopes = getFromQuery(EventStore.Query.builder().withEntityId("bjorn@gmail.com").build());

        assertThat(eventEnvelopes).isEqualTo(events);

    }


    @Test
    public void insertAndRollback() {

        reactiveTransactionManager = new ReactiveTransactionManager(pgAsyncPool);
        LocalDateTime emissionDate = LocalDateTime.now();
        List<EventEnvelope<VikingEvent, Void, Void>> events = API.List(
                eventEnvelope(1L, new VikingEvent.VikingCreated("bjorn@gmail.com"), emissionDate),
                eventEnvelope(2L, new VikingEvent.VikingUpdated("bjorn@gmail.com"), emissionDate),
                eventEnvelope(3L, new VikingEvent.VikingDeleted("bjorn@gmail.com"), emissionDate)
        );
        Try.of(() ->
                reactiveTransactionManager.withTransaction(connection ->
                        postgresEventStore.persist(connection, events)
                            .map(__ -> {
                                println("Throwing exception");
                                throw new RuntimeException();
                            })
                ).get()
        ).onFailure(e -> {
            e.printStackTrace();
        });

        int count = this.dslContext.fetchCount(vikings_journal);
        assertThat(count).isEqualTo(0);
    }


    @Test
    public void nextSequence() {
        Long seq = pgAsyncPool.inTransaction(ctx -> postgresEventStore.nextSequence(ctx)).get();

        assertThat(seq).isNotNull();

    }

    @Test
    public void insertDateNull() {
        List<EventEnvelope<VikingEvent, Void, Void>> events = API.List(
                eventEnvelope(1L, new VikingEvent.VikingCreated("bjorn@gmail.com"), null)
        );
        pgAsyncPool.inTransaction(ctx -> postgresEventStore.persist(ctx, events))
                .get();
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
        EventEnvelope<VikingEvent, Void, Void> event1Updated = postgresEventStore.markAsPublished(event1).get();
        List<EventEnvelope<VikingEvent, Void, Void>> events = getFromQuery(EventStore.Query.builder().withPublished(true).build());
        assertThat(events).containsExactlyInAnyOrder(event1Updated);
    }


    @Test
    public void loadEventsUnpublished() {
        initDatas();
        List<EventEnvelope<VikingEvent, Void, Void>> events = List.ofAll(transactionSource()
                .flatMapConcat(t -> postgresEventStore
                        .loadEventsUnpublished(t, EventStore.ConcurrentReplayStrategy.NO_STRATEGY)
                        .watchTermination((nu, d) -> d.whenComplete((__, e) -> postgresEventStore.commitOrRollback(Option.of(e), t)))
                )
                .runWith(Sink.seq(), Materializer.createMaterializer(system)).toCompletableFuture().join()
        );

        assertThat(events).containsExactlyInAnyOrder(event1, event2, event3, event4, event5, event6);
    }

    @Test
    public void loadEventsUnpublishedSkip() throws InterruptedException {
        initDatas();
        CompletionStage<java.util.List<EventEnvelope<VikingEvent, Void, Void>>> first = transactionSource().flatMapConcat(t ->
                postgresEventStore.loadEventsUnpublished(t, SKIP)
                        .flatMapConcat(elt -> Source.tick(Duration.ofMillis(100), Duration.ofMillis(100), elt).take(1))
                        .watchTermination((nu, d) -> d.whenComplete((__, e) -> postgresEventStore.commitOrRollback(Option.of(e), t)))
        ).runWith(Sink.seq(), Materializer.createMaterializer(system));
        Thread.sleep(100);
        long start = System.currentTimeMillis();
        CompletionStage<java.util.List<EventEnvelope<VikingEvent, Void, Void>>> second = transactionSource().flatMapConcat(t ->
                postgresEventStore.loadEventsUnpublished(t, SKIP).watchTermination((nu, d) -> d.whenComplete((__, e) -> postgresEventStore.commitOrRollback(Option.of(e), t)))
        ).runWith(Sink.seq(), Materializer.createMaterializer(system));

        List<EventEnvelope<VikingEvent, Void, Void>> events2 = List.ofAll(second.toCompletableFuture().join());
        long took = System.currentTimeMillis() - start;

        List<EventEnvelope<VikingEvent, Void, Void>> events1 = List.ofAll(first.toCompletableFuture().join());
        assertThat(events1).containsExactlyInAnyOrder(event1, event2, event3, event4, event5, event6);
        assertThat(events2).isEmpty();
        assertThat(took).isLessThan(500);
    }

    @Test
    public void loadEventsUnpublishedWait() throws InterruptedException {
        initDatas();
        CompletionStage<java.util.List<EventEnvelope<VikingEvent, Void, Void>>> first = transactionSource().flatMapConcat(t ->
                postgresEventStore.loadEventsUnpublished(t, WAIT)
                        .flatMapConcat(elt -> Source.tick(Duration.ofMillis(100), Duration.ofMillis(100), elt).take(1))
                        .flatMapConcat(e -> Source.completionStage(postgresEventStore.markAsPublished(t, e).toCompletableFuture()).map(__ -> e))
                        .watchTermination((nu, d) -> d.whenComplete((__, e) -> t.commit()))
        ).runWith(Sink.seq(), Materializer.createMaterializer(system));
        Thread.sleep(100);
        long start = System.currentTimeMillis();
        CompletionStage<java.util.List<EventEnvelope<VikingEvent, Void, Void>>> second = transactionSource().flatMapConcat(t ->
                postgresEventStore.loadEventsUnpublished(t, WAIT).watchTermination((nu, d) -> d.whenComplete((__, e) -> postgresEventStore.commitOrRollback(Option.of(e), t)))
        ).runWith(Sink.seq(), Materializer.createMaterializer(system));

        List<EventEnvelope<VikingEvent, Void, Void>> events2 = List.ofAll(second.toCompletableFuture().join());
        long took = System.currentTimeMillis() - start;

        List<EventEnvelope<VikingEvent, Void, Void>> events1 = List.ofAll(first.toCompletableFuture().join());
        assertThat(events1).containsExactlyInAnyOrder(event1, event2, event3, event4, event5, event6);
        assertThat(events2).isEmpty();
        assertThat(took).isGreaterThan(600);
    }

    private Source<PgAsyncTransaction, NotUsed> transactionSource() {
        return Source.completionStage(postgresEventStore.openTransaction().toCompletableFuture());
    }

    @Test
    public void markEventsAsPublished() {
        initDatas();
        List<EventEnvelope<VikingEvent, Void, Void>> events = List.ofAll(
                transactionSource().flatMapConcat(t ->
                        postgresEventStore.loadEventsUnpublished(t, SKIP).watchTermination((nu, d) -> d.whenComplete((__, e) -> t.commit()))
                ).runWith(Sink.seq(), Materializer.createMaterializer(system)).toCompletableFuture().join()
        );
        assertThat(events).containsExactlyInAnyOrder(event1, event2, event3, event4, event5, event6);
        postgresEventStore.markAsPublished(events).get();

        List<EventEnvelope<VikingEvent, Void, Void>> published = List.ofAll(
                transactionSource().flatMapConcat(t ->
                        postgresEventStore.loadEventsUnpublished(t, SKIP).watchTermination((nu, d) -> d.whenComplete((__, e) -> t.commit()))
                ).runWith(Sink.seq(), Materializer.createMaterializer(system)).toCompletableFuture().join()
        );
        assertThat(published).isEmpty();
    }

    private List<EventEnvelope<VikingEvent, Void, Void>> getFromQuery(EventStore.Query query) {
        return List.ofAll(postgresEventStore.loadEventsByQuery(query).runWith(Sink.seq(), Materializer.createMaterializer(system)).toCompletableFuture().join());
    }

    private void initDatas() {
        List<EventEnvelope<VikingEvent, Void, Void>> events = API.List(event1, event2, event3, event4, event5, event6);
        pgAsyncPool.inTransaction(ctx -> postgresEventStore.persist(ctx, events))
                .get();
    }

    protected abstract PgAsyncPool init();

    @BeforeEach
    public void setUp() {

        EventPublisher<VikingEvent, Void, Void> eventPublisher = mock(EventPublisher.class);

        this.system = ActorSystem.create();

        this.pgAsyncPool = init();

        PGSimpleDataSource pgSimpleDataSource = new PGSimpleDataSource();
        pgSimpleDataSource.setUrl("jdbc:postgresql://"+host+":"+port+"/"+database);
        pgSimpleDataSource.setUser(user);
        pgSimpleDataSource.setPassword(password);
        this.dslContext = DSL.using(pgSimpleDataSource, SQLDialect.POSTGRES);
        Try.of(() -> {
            this.dslContext.deleteFrom(vikings_journal).execute();
            return "";
        });
        try (Connection connection = pgSimpleDataSource.getConnection()) {
            executeSqlScript(connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        this.postgresEventStore = new ReactivePostgresEventStore<>(
                system,
                eventPublisher,
                this.pgAsyncPool,
                new TableNames(tableName(), tableName()+"_sequence_num"),
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

        Type<VikingCreated> VikingCreatedV1 = Type.create(VikingEvent.VikingCreated.class, 1L);
        Type<VikingUpdated> VikingUpdatedV1 = Type.create(VikingEvent.VikingUpdated.class, 1L);
        Type<VikingDeleted> VikingDeletedV1 = Type.create(VikingEvent.VikingDeleted.class, 1L);

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

    public void executeSqlScript(Connection conn) {
        // Delimiter
//        String delimiter = ";";

        // Create scanner
//        Scanner scanner;
//        InputStream file = ReactivePostgresEventStoreTest.class.getClassLoader().getResourceAsStream("base.sql");
//        scanner = new Scanner(file).useDelimiter(delimiter);

        String script = "CREATE TABLE IF NOT EXISTS "+tableName()+" ( \n" +
                "  id UUID primary key,\n" +
                "  entity_id varchar(100) not null,\n" +
                "  sequence_num bigint not null,\n" +
                "  event_type varchar(100) not null,\n" +
                "  version int not null,\n" +
                "  transaction_id varchar(100) not null,\n" +
                "  event jsonb not null,\n" +
                "  metadata jsonb,\n" +
                "  context jsonb,\n" +
                "  total_message_in_transaction int default 1,\n" +
                "  num_message_in_transaction int default 1,\n" +
                "  emission_date timestamp not null default now(),\n" +
                "  user_id varchar(100),\n" +
                "  system_id varchar(100),\n" +
                "  published boolean default false,\n" +
                "  UNIQUE (entity_id, sequence_num)\n" +
                ");\n" +
                "CREATE INDEX IF NOT EXISTS "+tableName()+"_sequence_num_idx    ON "+tableName()+" (sequence_num);\n" +
                "CREATE INDEX IF NOT EXISTS "+tableName()+"_entity_id_idx       ON "+tableName()+" (entity_id);\n" +
                "CREATE INDEX IF NOT EXISTS "+tableName()+"_user_id_idx         ON "+tableName()+" (user_id);\n" +
                "CREATE INDEX IF NOT EXISTS "+tableName()+"_system_id_idx       ON "+tableName()+" (system_id);\n" +
                "CREATE INDEX IF NOT EXISTS "+tableName()+"_emission_date_idx   ON "+tableName()+" (emission_date);\n" +
                "CREATE SEQUENCE if not exists "+tableName()+"_id;\n" +
                "CREATE SEQUENCE if not exists "+tableName()+"_sequence_num;\n" +
                "CREATE SEQUENCE if not exists "+tableName()+"_sequence_id;\n";

        // Loop through the SQL file statements
        Statement currentStatement = null;
//        while(scanner.hasNext()) {
//            // Get statement
//            String rawStatement = scanner.next() + delimiter;
            try {
                // Execute statement
                currentStatement = conn.createStatement();
                currentStatement.execute(script);
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
//        }
//        scanner.close();
    }

}
