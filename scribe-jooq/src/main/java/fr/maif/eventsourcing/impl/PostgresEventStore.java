package fr.maif.eventsourcing.impl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.Materializer;
import akka.stream.javadsl.Source;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import fr.maif.json.MapperSingleton;
import io.vavr.Tuple0;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventPublisher;
import fr.maif.eventsourcing.EventStore;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.jdbc.Sql;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.Seq;
import io.vavr.concurrent.Future;
import io.vavr.control.Either;
import io.vavr.control.Option;
import io.vavr.control.Try;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

import static io.vavr.API.Seq;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.function.Function.identity;
import static org.jooq.impl.DSL.*;

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


    private final ActorSystem system;
    private final Materializer materializer;
    private final DataSource dataSource;
    private final ExecutorService executor;
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

    public PostgresEventStore(ActorSystem system, EventPublisher<E, Meta, Context> eventPublisher, DataSource dataSource, ExecutorService executor, TableNames tableNames, JacksonEventFormat<?, E> eventFormat, JacksonSimpleFormat<Meta>  metaFormat, JacksonSimpleFormat<Context>  contextFormat) {
        this.system = system;
        this.materializer = Materializer.createMaterializer(system);
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
            ActorSystem system,
            EventPublisher<E, Meta, Context> eventPublisher,
            DataSource dataSource,
            ExecutorService executor,
            TableNames tableNames,
            JacksonEventFormat<?, E> eventFormat) {
        return new PostgresEventStore<>(system, eventPublisher, dataSource, executor, tableNames, eventFormat, JacksonSimpleFormat.empty(), JacksonSimpleFormat.empty());
    }

    @Override
    public void close() throws IOException {
        this.eventPublisher.close();
    }

    @Override
    public ActorSystem system() {
        return this.system;
    }

    @Override
    public Materializer materializer() {
        return this.materializer;
    }

    @Override
    public Future<Tuple0> persist(Connection connection, List<EventEnvelope<E, Meta, Context>> events) {

        return Future.of(executor, () -> {
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
        });
    }

    @Override
    public Future<Long> nextSequence(Connection tx) {
        return Future.of(executor, () ->
                DSL.using(tx).nextval(name(this.tableNames.sequenceNumName)).longValue()
        );
    }

    @Override
    public Future<Tuple0> publish(List<EventEnvelope<E, Meta, Context>> events) {
        return this.eventPublisher.publish(events);
    }

    @Override
    public Future<EventEnvelope<E, Meta, Context>> markAsPublished(EventEnvelope<E, Meta, Context> eventEnvelope) {
        return Future.fromCompletableFuture(sql.update(table(this.tableNames.tableName))
                .set(PUBLISHED, true)
                .where(ID.eq(eventEnvelope.id))
                .executeAsync(executor)
                .toCompletableFuture()
        ).map(__ -> eventEnvelope.copy().withPublished(true).build());
    }

    @Override
    public Future<List<EventEnvelope<E, Meta, Context>>> markAsPublished(List<EventEnvelope<E, Meta, Context>> eventEnvelopes) {
        return Future.fromCompletableFuture(
            sql.update(table(this.tableNames.tableName))
                .set(PUBLISHED, true)
                .where(ID.in(eventEnvelopes.map(evt -> evt.id).toJavaArray(UUID[]::new)))
                .executeAsync(executor)
                .toCompletableFuture()
        ).map(__ -> eventEnvelopes.map(eventEnvelope -> eventEnvelope.copy().withPublished(true).build()));
    }

    @Override
    public Source<EventEnvelope<E, Meta, Context>, NotUsed> loadEventsUnpublished() {
        return Sql.connection(dataSource, executor, false)
                .flatMapConcat(c ->
                        Sql.of(c, system)
                                .select(SELECT_CLAUSE + " FROM "+this.tableNames.tableName+" WHERE published = false ")
                                .as(this::rsToEnvelope)
                                .get()
                );
    }

    @Override
    public Source<EventEnvelope<E, Meta, Context>, NotUsed> loadEventsByQuery(Connection tx, Query query) {
        return loadEventsByQueryWithOptions(tx, query, false);
    }

    public Source<EventEnvelope<E, Meta, Context>, NotUsed> loadEventsByQueryWithOptions(Connection tx, Query query, boolean autoClose) {
        Seq<Tuple2<String, Object>> clauses = Seq(
                query.dateFrom().map(d -> Tuple.<String, Object>of(" emission_date > ? ", Timestamp.valueOf(d))),
                query.dateTo().map(d -> Tuple.<String, Object>of(" emission_date < ? ", Timestamp.valueOf(d))),
                query.entityId().map(d -> Tuple.<String, Object>of(" entity_id = ? ", d)),
                query.systemId().map(d -> Tuple.<String, Object>of(" system_id = ? ", d)),
                query.userId().map(d -> Tuple.<String, Object>of(" user_id = ? ", d)),
                query.published().map(d -> Tuple.<String, Object>of(" published = ? ", d)),
                query.sequenceTo().map(d -> Tuple.<String, Object>of(" sequence_num <= ? ", d)),
                query.sequenceFrom().map(d -> Tuple.<String, Object>of(" sequence_num >= ? ", d))
        )
                .flatMap(identity());

        String strClauses = clauses.map(Tuple2::_1).mkString("WHERE", " AND ", "");
        String select = SELECT_CLAUSE + " FROM " + this.tableNames.tableName + " " + (clauses.isEmpty() ? "" : strClauses) + " ORDER BY sequence_num asc";

        Object[] objects = clauses.map(Tuple2::_2).toJavaArray();

        LOGGER.debug("{}", select);

        return  Sql.of(tx, system)
                    .select(select)
                    .closeConnection(autoClose)
                    .params(objects)
                    .as(this::rsToEnvelope)
                    .get();
    }

    @Override
    public Source<EventEnvelope<E, Meta, Context>, NotUsed> loadEventsByQuery(Query query) {
        return Sql.connection(dataSource, executor, false)
                .flatMapConcat(c ->
                        loadEventsByQueryWithOptions(c, query, true)
                );
    }

    private EventEnvelope<E, Meta, Context> rsToEnvelope(ResultSet rs) {
        return Try
                .of(() -> {
                    String event_type = rs.getString("event_type");
                    long version = rs.getLong("version");
                    JsonNode event = readValue(rs.getString("event")).getOrElse(NullNode.getInstance());
                    Either<?, E> eventRead = eventFormat.read(event_type, version, event);
                    eventRead.left().forEach(err -> {
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
