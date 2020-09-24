package fr.maif.eventsourcing;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Source;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.jooq.PgAsyncPool;
import fr.maif.jooq.PgAsyncTransaction;
import fr.maif.jooq.QueryResult;
import io.vavr.API;
import io.vavr.collection.List;
import io.vavr.collection.Seq;
import io.vavr.concurrent.Future;
import io.vavr.control.Either;
import io.vavr.control.Option;
import io.vavr.control.Try;
import org.jooq.*;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

import static java.util.function.Function.identity;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

public class ReactivePostgresEventStore<E extends Event, Meta, Context> implements EventStore<PgAsyncTransaction, E, Meta, Context>, Closeable {


    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ReactivePostgresEventStore.class);

    private final static Field<UUID> ID = field("id", UUID.class);
    private final static Field<String> ENTITY_ID = field("entity_id", String.class);
    private final static Field<Long> SEQUENCE_NUM = field("sequence_num", Long.class);
    private final static Field<String> EVENT_TYPE = field("event_type", String.class);
    private final static Field<Long> VERSION = field("version", Long.class);
    private final static Field<String> TRANSACTION_ID = field("transaction_id", String.class);
    private final static Field<String> EVENT = field("event", SQLDataType.OTHER.asConvertedDataType(new StringJsonBinding()));
    private final static Field<String> METADATA = field("metadata", SQLDataType.OTHER.asConvertedDataType(new StringJsonBinding()));
    private final static Field<String> CONTEXT = field("context", SQLDataType.OTHER.asConvertedDataType(new StringJsonBinding()));
    private final static Field<Integer> TOTAL_MESSAGE_IN_TRANSACTION = field("total_message_in_transaction", Integer.class);
    private final static Field<Integer> NUM_MESSAGE_IN_TRANSACTION = field("num_message_in_transaction", Integer.class);
    private final static Field<String> USER_ID = field("user_id", String.class);
    private final static Field<String> SYSTEM_ID = field("system_id", String.class);
    private final static Field<LocalDateTime> EMISSION_DATE = field("emission_date", LocalDateTime.class);
    private final static Field<Boolean> PUBLISHED = field("published", Boolean.class);

    private final ActorSystem system;
    private final Materializer materializer;
    private final PgAsyncPool pgAsyncPool;
    private final TableNames tableNames;
    private final EventPublisher<E, Meta, Context> eventPublisher;
    private final JacksonEventFormat<?, E> eventFormat;
    private final JacksonSimpleFormat<Meta> metaFormat;
    private final JacksonSimpleFormat<Context> contextFormat;
    private final ObjectMapper objectMapper;

    public ReactivePostgresEventStore(
            ActorSystem system,
            EventPublisher<E, Meta, Context> eventPublisher,
            PgAsyncPool pgAsyncPool,
            TableNames tableNames, JacksonEventFormat<?, E> eventFormat,
            JacksonSimpleFormat<Meta> metaFormat,
            JacksonSimpleFormat<Context> contextFormat) {

        this.system = system;
        this.materializer = ActorMaterializer.create(system);
        this.pgAsyncPool = pgAsyncPool;
        this.tableNames = tableNames;
        this.eventPublisher = eventPublisher;
        this.eventFormat = eventFormat;
        this.metaFormat = metaFormat;
        this.contextFormat = contextFormat;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public ActorSystem system() {
        return system;
    }

    @Override
    public Materializer materializer() {
        return materializer;
    }

    @Override
    public Future<Tuple0> persist(PgAsyncTransaction transactionContext, List<EventEnvelope<E, Meta, Context>> events) {
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
                SYSTEM_ID,
                EMISSION_DATE
        );

        return transactionContext.executeBatch(dslContext ->
                        dslContext.insertInto(table(this.tableNames.tableName)).columns(fields.toJavaList())
                                .values(
                                        fields.map(f -> null).toJavaList()
                                ),
                events.map(event -> {
                    String eventString = Try.of(() -> objectMapper.writeValueAsString(eventFormat.write(event.event))).get();
                    String contextString = contextFormat.write(Option.of(event.context))
                            .flatMap(c -> Try.of(() -> objectMapper.writeValueAsString(c)).toOption())
                            .getOrNull();
                    String metaString = metaFormat.write(Option.of(event.metadata))
                            .flatMap(m -> Try.of(() -> objectMapper.writeValueAsString(m)).toOption())
                            .getOrNull();
                    LocalDateTime emissionDate = Option.of(event.emissionDate).getOrElse(LocalDateTime.now());
                    return List.of(
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
                            event.systemId,
                            emissionDate
                    );
                })
        ).map(__ -> Tuple.empty());
    }

    @Override
    public Source<EventEnvelope<E, Meta, Context>, NotUsed> loadEventsUnpublished() {
        return pgAsyncPool.stream(500, dsl ->
                dsl.select(
                        ID,
                        ENTITY_ID,
                        SEQUENCE_NUM,
                        EVENT_TYPE,
                        VERSION,
                        TRANSACTION_ID,
                        EVENT,
                        METADATA,
                        EMISSION_DATE,
                        USER_ID,
                        SYSTEM_ID,
                        TOTAL_MESSAGE_IN_TRANSACTION,
                        NUM_MESSAGE_IN_TRANSACTION,
                        CONTEXT,
                        PUBLISHED
                ).from(table(this.tableNames.tableName)).where(PUBLISHED.isFalse())
        ).map(this::rsToEnvelope);
    }

    @Override
    public Source<EventEnvelope<E, Meta, Context>, NotUsed> loadEventsByQuery(PgAsyncTransaction tx, Query query) {

        Seq<Condition> clauses = API.Seq(
                query.dateFrom().map(EMISSION_DATE::gt),
                query.dateTo().map(EMISSION_DATE::lt),
                query.entityId().map(ENTITY_ID::eq),
                query.systemId().map(SYSTEM_ID::eq),
                query.userId().map(USER_ID::eq),
                query.published().map(PUBLISHED::eq),
                query.sequenceTo().map(SEQUENCE_NUM::le),
                query.sequenceFrom().map(SEQUENCE_NUM::ge)
        ).flatMap(identity());

        return tx.stream(500, dsl -> dsl
                .select(
                        ID,
                        ENTITY_ID,
                        SEQUENCE_NUM,
                        EVENT_TYPE,
                        VERSION,
                        TRANSACTION_ID,
                        EVENT,
                        METADATA,
                        EMISSION_DATE,
                        USER_ID,
                        SYSTEM_ID,
                        TOTAL_MESSAGE_IN_TRANSACTION,
                        NUM_MESSAGE_IN_TRANSACTION,
                        CONTEXT,
                        PUBLISHED)
                .from(table(this.tableNames.tableName))
                .where(clauses.toJavaList())
                .orderBy(SEQUENCE_NUM)
        ).map(this::rsToEnvelope);
    }

    @Override
    public Source<EventEnvelope<E, Meta, Context>, NotUsed> loadEventsByQuery(Query query) {
        return Source.fromSourceCompletionStage(this.pgAsyncPool.begin().map(ctx ->
                loadEventsByQuery(ctx, query)
                        .watchTermination((nu, d) ->
                                    d.handleAsync((__, e) -> {
                                        if (e != null) {
                                            LOGGER.error("loadEventsByQuery terminated with error", e);
                                            return ctx.rollback().toCompletableFuture();
                                        } else {
                                            LOGGER.debug("loadEventsByQuery terminated correctly");
                                            return ctx.commit().toCompletableFuture();
                                        }
                                    })
                        )
        ).toCompletableFuture()).mapMaterializedValue(__ -> NotUsed.notUsed());
    }

    @Override
    public Future<Long> nextSequence(PgAsyncTransaction tx) {
        return tx.queryOne(dsl ->
                dsl.resultQuery("select nextval('" + this.tableNames.sequenceNumName + "')")
        ).map(mayBeResult -> mayBeResult.map(r -> r.get(0, Long.class)).getOrNull());
    }

    @Override
    public Future<Tuple0> publish(List<EventEnvelope<E, Meta, Context>> events) {
        LOGGER.debug("Publishing event {}", events);
        return this.eventPublisher.publish(events);
    }

    @Override
    public Future<EventEnvelope<E, Meta, Context>> markAsPublished(EventEnvelope<E, Meta, Context> eventEnvelope) {
        return pgAsyncPool.execute(dsl -> dsl
                .update(table(this.tableNames.tableName))
                .set(PUBLISHED, true)
                .where(ID.eq(eventEnvelope.id))
        ).map(__ -> eventEnvelope.copy().withPublished(true).build());
    }

    @Override
    public Future<List<EventEnvelope<E, Meta, Context>>> markAsPublished(List<EventEnvelope<E, Meta, Context>> eventEnvelopes) {
        return pgAsyncPool.execute(dsl -> dsl
                .update(table(this.tableNames.tableName))
                .set(PUBLISHED, true)
                .where(ID.in(eventEnvelopes.map(evt -> evt.id).toJavaArray(UUID[]::new)))
        ).map(__ -> eventEnvelopes.map(eventEnvelope -> eventEnvelope.copy().withPublished(true).build()));
    }

    @Override
    public void close() throws IOException {

    }

    private EventEnvelope<E, Meta, Context> rsToEnvelope(QueryResult rs) {
        return Try
                .of(() -> {
                    String event_type = rs.get(EVENT_TYPE);
                    long version = rs.get(VERSION);
                    JsonNode event = readValue(rs.get(EVENT)).getOrElse(NullNode.getInstance());
                    Either<?, E> eventRead = eventFormat.read(event_type, version, event);
                    eventRead.left().forEach(err -> {
                        LOGGER.error("Error reading event {} : {}", event, err);
                    });
                    EventEnvelope.Builder<E, Meta, Context> builder = EventEnvelope.<E, Meta, Context>builder()
                            .withId(rs.get(ID))
                            .withEntityId(rs.get(ENTITY_ID))
                            .withSequenceNum(rs.get(SEQUENCE_NUM))
                            .withEventType(event_type)
                            .withVersion(version)
                            .withTransactionId(rs.get(TRANSACTION_ID))
                            .withEvent(eventRead.get())
                            .withEmissionDate(rs.get(EMISSION_DATE))
                            .withPublished(rs.get(PUBLISHED))
                            .withSystemId(rs.get(SYSTEM_ID))
                            .withUserId(rs.get(USER_ID))
                            .withPublished(rs.get(PUBLISHED))
                            .withNumMessageInTransaction(rs.get(NUM_MESSAGE_IN_TRANSACTION))
                            .withTotalMessageInTransaction(rs.get(TOTAL_MESSAGE_IN_TRANSACTION));

                    metaFormat.read(readValue(rs.get(METADATA))).forEach(builder::withMetadata);
                    contextFormat.read(readValue(rs.get(CONTEXT))).forEach(builder::withContext);
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


    public static class StringJsonBinding implements Binding<Object, String> {

        // The converter does all the work
        @Override
        public Converter<Object, String> converter() {
            return new Converter<Object, String>() {
                @Override
                public String from(Object t) {
                    return t == null ? null : t.toString();
                }

                @Override
                public Object to(String u) {
                    return u;
                }

                @Override
                public Class<Object> fromType() {
                    return Object.class;
                }

                @Override
                public Class<String> toType() {
                    return String.class;
                }
            };
        }

        // Rending a bind variable for the binding context's value and casting it to the json type
        @Override
        public void sql(BindingSQLContext<String> ctx) throws SQLException {
            // Depending on how you generate your SQL, you may need to explicitly distinguish
            // between jOOQ generating bind variables or inlined literals.
            if (ctx.render().paramType() == ParamType.INLINED) {
                ctx.render().visit(DSL.inline(ctx.convert(converter()).value())).sql("::jsonb");
            } else if (ctx.render().paramType() == ParamType.NAMED) {
                ctx.render().visit(DSL.query(ctx.variable() + "::jsonb"));
            } else if (ctx.render().paramType() == ParamType.INDEXED) {
                ctx.render().visit(DSL.query(ctx.variable() + "::jsonb"));
            } else if (ctx.render().paramType() == ParamType.FORCE_INDEXED) {
                ctx.render().visit(DSL.query(ctx.variable() + "::jsonb"));
            } else {
                ctx.render().sql("?::jsonb");
            }
        }

        // Registering VARCHAR types for JDBC CallableStatement OUT parameters
        @Override
        public void register(BindingRegisterContext<String> ctx) throws SQLException {
            ctx.statement().registerOutParameter(ctx.index(), Types.VARCHAR);
        }

        // Converting the JsValue to a String value and setting that on a JDBC PreparedStatement
        @Override
        public void set(BindingSetStatementContext<String> ctx) throws SQLException {
            ctx.statement().setString(ctx.index(), Objects.toString(ctx.convert(converter()).value(), null));
        }

        // Getting a String value from a JDBC ResultSet and converting that to a JsValue
        @Override
        public void get(BindingGetResultSetContext<String> ctx) throws SQLException {
            ctx.convert(converter()).value(ctx.resultSet().getString(ctx.index()));
        }

        // Getting a String value from a JDBC CallableStatement and converting that to a JsValue
        @Override
        public void get(BindingGetStatementContext<String> ctx) throws SQLException {
            ctx.convert(converter()).value(ctx.statement().getString(ctx.index()));
        }

        // Setting a value on a JDBC SQLOutput (useful for Oracle OBJECT types)
        @Override
        public void set(BindingSetSQLOutputContext<String> ctx) throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        // Getting a value from a JDBC SQLInput (useful for Oracle OBJECT types)
        @Override
        public void get(BindingGetSQLInputContext<String> ctx) throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }
    }
}
