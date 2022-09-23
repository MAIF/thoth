package fr.maif.eventsourcing.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import fr.maif.concurrent.CompletionStages;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventPublisher;
import fr.maif.eventsourcing.EventStore;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.json.MapperSingleton;
import io.vavr.Tuple;
import io.vavr.collection.List;
import io.vavr.collection.Seq;
import io.vavr.collection.Traversable;
import io.vavr.control.Either;
import io.vavr.control.Option;
import io.vavr.control.Try;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.reactivestreams.Publisher;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static io.vavr.API.List;
import static io.vavr.API.Seq;
import static java.util.function.Function.identity;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;

public class PostgresEventStore<E extends Event, Meta, Context> implements EventStore<Connection, E, Meta, Context>, Closeable {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PostgresEventStore.class);

    private final static Field<UUID> ID = field("id", UUID.class);
    private final static Field<String> ENTITY_ID = field("entity_id", String.class);
    private final static Field<Long> SEQUENCE_NUM = field("sequence_num", Long.class);
    private final static Field<String> EVENT_TYPE = field("event_type", String.class);
    private final static Field<Long> VERSION = field("version", Long.class);
    private final static Field<String> TRANSACTION_ID = field("transaction_id", String.class);
    private final static Field<String> EVENT = field("event", String.class);
    private final static Field<String> METADATA = field("metadata", String.class);
    private final static Field<String> CONTEXT = field("context", String.class);
    private final static Field<Integer> TOTAL_MESSAGE_IN_TRANSACTION = field("total_message_in_transaction", Integer.class);
    private final static Field<Integer> NUM_MESSAGE_IN_TRANSACTION = field("num_message_in_transaction", Integer.class);
    private final static Field<String> USER_ID = field("user_id", String.class);
    private final static Field<String> SYSTEM_ID = field("system_id", String.class);
    private final static Field<Timestamp> EMISSION_DATE = field("emission_date", Timestamp.class);
    private final static Field<Boolean> PUBLISHED = field("published", Boolean.class);

    private final DataSource dataSource;
    private final Executor executor;
    private final TableNames tableNames;
    private final EventPublisher<E, Meta, Context> eventPublisher;
    private final DSLContext sql;
    private final JacksonEventFormat<?, E> eventFormat;
    private final JacksonSimpleFormat<Meta> metaFormat;
    private final JacksonSimpleFormat<Context> contextFormat;
    private final ObjectMapper objectMapper;
    private final static String SELECT_CLAUSE =
            "SELECT " +
                    "  id," +
                    "  entity_id," +
                    "  sequence_num," +
                    "  event_type," +
                    "  version," +
                    "  transaction_id," +
                    "  event," +
                    "  metadata," +
                    "  emission_date," +
                    "  user_id," +
                    "  system_id," +
                    "  total_message_in_transaction," +
                    "  num_message_in_transaction," +
                    "  context," +
                    "  published ";

    public PostgresEventStore(EventPublisher<E, Meta, Context> eventPublisher, DataSource dataSource, Executor executor, TableNames tableNames, JacksonEventFormat<?, E> eventFormat, JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat) {
        this.dataSource = dataSource;
        this.executor = executor;
        this.tableNames = tableNames;
        this.sql = DSL.using(dataSource, SQLDialect.POSTGRES);
        this.eventPublisher = eventPublisher;
        this.eventFormat = eventFormat;
        this.metaFormat = metaFormat;
        this.contextFormat = contextFormat;
        this.objectMapper = MapperSingleton.getInstance();
    }

    public static <E extends Event, Meta, Context> PostgresEventStore<E, Meta, Context> create(
            EventPublisher<E, Meta, Context> eventPublisher,
            DataSource dataSource,
            ExecutorService executor,
            TableNames tableNames,
            JacksonEventFormat<?, E> eventFormat) {
        return new PostgresEventStore<>(eventPublisher, dataSource, executor, tableNames, eventFormat, JacksonSimpleFormat.empty(), JacksonSimpleFormat.empty());
    }

    @Override
    public void close() throws IOException {
        this.eventPublisher.close();
    }

    @Override
    public CompletionStage<Connection> openTransaction() {
        return CompletionStages.fromTry(() -> Try.of(() -> {
            Connection connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            return connection;
        }), executor);
    }

    @Override
    public CompletionStage<Void> commitOrRollback(Option<Throwable> mayBeCrash, Connection connection) {
        return mayBeCrash.fold(
                () -> CompletionStages.fromTry(() -> Try.of(() -> {
                    connection.commit();
                    connection.close();
                    return Tuple.empty();
                }), executor),
                e -> CompletionStages.fromTry(() -> Try.of(() -> {
                    connection.rollback();
                    connection.close();
                    return Tuple.empty();
                }), executor)
        ).thenRun(() -> {
        });
    }

    @Override
    public CompletionStage<Void> persist(Connection connection, List<EventEnvelope<E, Meta, Context>> events) {

        return CompletionStages.fromTry(() -> Try.of(() -> {
            DSLContext create = DSL.using(connection, SQLDialect.POSTGRES);

            create.batch(
                    events.map(event -> {
                                String eventString = Try.of(() -> objectMapper.writeValueAsString(eventFormat.write(event.event))).get();
                                String contextString = contextFormat.write(Option.of(event.context))
                                        .flatMap(c -> Try.of(() -> objectMapper.writeValueAsString(c)).toOption())
                                        .getOrNull();
                                String metaString = metaFormat.write(Option.of(event.metadata))
                                        .flatMap(m -> Try.of(() -> objectMapper.writeValueAsString(m)).toOption())
                                        .getOrNull();

                                List<Field<?>> fields = List.of(
                                        ID,
                                        ENTITY_ID,
                                        SEQUENCE_NUM,
                                        EVENT_TYPE,
                                        VERSION,
                                        TRANSACTION_ID,
                                        EVENT,
                                        METADATA,
                                        CONTEXT,
                                        TOTAL_MESSAGE_IN_TRANSACTION,
                                        NUM_MESSAGE_IN_TRANSACTION,
                                        USER_ID,
                                        SYSTEM_ID
                                );

                                List<Object> values = List.of(
                                        event.id,
                                        event.entityId,
                                        event.sequenceNum,
                                        event.eventType,
                                        event.version,
                                        event.transactionId,
                                        eventString,
                                        metaString,
                                        contextString,
                                        event.totalMessageInTransaction,
                                        event.numMessageInTransaction,
                                        event.userId,
                                        event.systemId
                                );
                                List<Field<?>> finalFields;
                                List<Object> finalValues;
                                if (event.emissionDate == null) {
                                    finalFields = fields;
                                    finalValues = values;
                                } else {
                                    finalFields = fields.append(EMISSION_DATE);
                                    finalValues = values.append(Timestamp.valueOf(event.emissionDate));
                                }
                                return create.insertInto(table(this.tableNames.tableName), finalFields.toJavaList()).values(finalValues.toJavaList());
                            }
                    ).toJavaList()
            ).execute();
            return Tuple.empty();
        }), executor).thenRun(() -> {
        });
    }

    @Override
    public CompletionStage<Long> nextSequence(Connection tx) {
        return CompletionStages.fromTry(() -> Try.of(() ->
                DSL.using(tx).nextval(name(this.tableNames.sequenceNumName)).longValue()
        ), executor);
    }

    @Override
    public CompletionStage<Void> publish(List<EventEnvelope<E, Meta, Context>> events) {
        return this.eventPublisher.publish(events);
    }


    @Override
    public CompletionStage<EventEnvelope<E, Meta, Context>> markAsPublished(EventEnvelope<E, Meta, Context> eventEnvelope) {
        return sql.update(table(this.tableNames.tableName))
                .set(PUBLISHED, true)
                .where(ID.eq(eventEnvelope.id))
                .executeAsync(executor)
                .toCompletableFuture()
                .thenApply(__ -> eventEnvelope.copy().withPublished(true).build());
    }

    @Override
    public CompletionStage<List<EventEnvelope<E, Meta, Context>>> markAsPublished(List<EventEnvelope<E, Meta, Context>> eventEnvelopes) {
        return sql.update(table(this.tableNames.tableName))
                .set(PUBLISHED, true)
                .where(ID.in(eventEnvelopes.map(evt -> evt.id).toJavaArray(UUID[]::new)))
                .executeAsync(executor)
                .toCompletableFuture()
                .thenApply(__ -> eventEnvelopes.map(eventEnvelope -> eventEnvelope.copy().withPublished(true).build()));
    }

    @Override
    public CompletionStage<List<EventEnvelope<E, Meta, Context>>> markAsPublished(Connection tx, List<EventEnvelope<E, Meta, Context>> eventEnvelopes) {
        return DSL.using(tx, SQLDialect.POSTGRES)
                .update(table(this.tableNames.tableName))
                .set(PUBLISHED, true)
                .where(ID.in(eventEnvelopes.map(evt -> evt.id).toJavaArray(UUID[]::new)))
                .executeAsync(executor)
                .toCompletableFuture()
                .thenApply(__ -> eventEnvelopes.map(eventEnvelope -> eventEnvelope.copy().withPublished(true).build()));
    }

    @Override
    public CompletionStage<EventEnvelope<E, Meta, Context>> markAsPublished(Connection tx, EventEnvelope<E, Meta, Context> eventEnvelope) {
        return markAsPublished(tx, List(eventEnvelope)).thenApply(Traversable::head);
    }

    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadEventsUnpublished(Connection c, ConcurrentReplayStrategy concurrentReplayStrategy) {
        String tmpQuery = SELECT_CLAUSE +
                " FROM " + this.tableNames.tableName +
                " WHERE published = false " +
                " order by sequence_num ";
        String query;
        switch (concurrentReplayStrategy) {
            case WAIT:
                query = tmpQuery + " for update of " + this.tableNames.tableName;
                break;
            case SKIP:
                query = tmpQuery + " for update of " + this.tableNames.tableName + " skip locked ";
                break;
            default:
                query = tmpQuery;
        }

        return Flux.fromStream(() -> DSL.using(c)
                .resultQuery(query)
                .stream()
                .map(r -> rsToEnvelope(r.intoResultSet()))
        );
    }

    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadEventsByQuery(Connection tx, Query query) {
        return loadEventsByQueryWithOptions(tx, query, false);
    }

    public Publisher<EventEnvelope<E, Meta, Context>> loadEventsByQueryWithOptions(Connection tx, Query query, boolean autoClose) {
        final Seq<Condition> clauses = Seq(
                query.dateFrom().map(d -> field("emission_date").greaterThan(Timestamp.valueOf(d))),
                query.dateTo().map(d -> field(" emission_date").lessThan(Timestamp.valueOf(d))),
                query.entityId().map(d -> field(" entity_id").eq(d)),
                query.systemId().map(d -> field(" system_id").eq(d)),
                query.userId().map(d -> field(" user_id").eq(d)),
                query.published().map(d -> field(" published").eq(d)),
                query.sequenceTo().map(d -> field(" sequence_num").lessOrEqual(d)),
                query.sequenceFrom().map(d -> field(" sequence_num").greaterOrEqual(d))
        ).flatMap(identity());

        var tmpJooqQuery = DSL.using(tx)
                .selectFrom(SELECT_CLAUSE + " FROM " + this.tableNames.tableName)
                .where(clauses.toJavaList())
                .orderBy(field("sequence_num").asc())
                ;
        var jooqQuery = Objects.nonNull(query.size) ? tmpJooqQuery.limit(query.size) : tmpJooqQuery;

        LOGGER.debug("{}", jooqQuery);
        return Flux.fromStream(() -> jooqQuery.stream().map(r -> rsToEnvelope(r.intoResultSet())))
                .doFinally(any -> {
                    if (autoClose) {
                        try {
                            tx.close();
                        } catch (SQLException e) {
                        }
                    }
                }).subscribeOn(Schedulers.fromExecutor(executor));
    }

    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadEventsByQuery(Query query) {
        return Flux.usingWhen(
                Mono.fromCallable(dataSource::getConnection).subscribeOn(Schedulers.fromExecutor(executor)),
                (Connection c) -> loadEventsByQueryWithOptions(c, query, true),
                c -> Mono.empty()
        );
    }

    private EventEnvelope<E, Meta, Context> rsToEnvelope(ResultSet rs) {
        return Try
                .of(() -> {
                    String event_type = rs.getString("event_type");
                    long version = rs.getLong("version");
                    JsonNode event = readValue(rs.getString("event")).getOrElse(NullNode.getInstance());
                    Either<?, E> eventRead = eventFormat.read(event_type, version, event);
                    eventRead.swap().forEach(err -> {
                        LOGGER.error("Error reading event {} : {}", event, err);
                    });
                    EventEnvelope.Builder<E, Meta, Context> builder = EventEnvelope.<E, Meta, Context>builder()
                            .withId(UUID.fromString(rs.getString("id")))
                            .withEntityId(rs.getString("entity_id"))
                            .withSequenceNum(rs.getLong("sequence_num"))
                            .withEventType(event_type)
                            .withVersion(version)
                            .withTransactionId(rs.getString("transaction_id"))
                            .withEvent(eventRead.get())
                            .withEmissionDate(rs.getTimestamp("emission_date").toLocalDateTime())
                            .withPublished(rs.getBoolean("published"))
                            .withSystemId(rs.getString("system_id"))
                            .withUserId(rs.getString("user_id"))
                            .withPublished(rs.getBoolean("published"))
                            .withNumMessageInTransaction(rs.getInt("num_message_in_transaction"))
                            .withTotalMessageInTransaction(rs.getInt("total_message_in_transaction"));

                    metaFormat.read(readValue(rs.getString("metadata"))).forEach(builder::withMetadata);
                    contextFormat.read(readValue(rs.getString("context"))).forEach(builder::withContext);
                    return builder.build();
                })
                .getOrElseThrow(e ->
                        new RuntimeException("Error reading event", e)
                );
    }

    private Option<JsonNode> readValue(String value) {
        return Option.of(value)
                .flatMap(str -> Try.of(() -> objectMapper.readTree(str)).toOption());
    }

}
