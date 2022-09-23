package fr.maif.eventsourcing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.jooq.PgAsyncPool;
import fr.maif.jooq.PgAsyncTransaction;
import fr.maif.jooq.QueryResult;
import fr.maif.json.Json;
import fr.maif.json.MapperSingleton;
import io.vavr.API;
import io.vavr.collection.List;
import io.vavr.collection.Seq;
import io.vavr.control.Either;
import io.vavr.control.Option;
import io.vavr.control.Try;
import org.jooq.Condition;
import org.jooq.Converter;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Record15;
import org.jooq.SelectSeekStep1;
import org.jooq.impl.SQLDataType;
import org.reactivestreams.Publisher;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.Closeable;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import static java.util.function.Function.identity;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

public class ReactivePostgresEventStore<Tx extends PgAsyncTransaction, E extends Event, Meta, Context> implements EventStore<Tx, E, Meta, Context>, Closeable {


    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ReactivePostgresEventStore.class);

    private final static Field<UUID> ID = field("id", UUID.class);
    private final static Field<String> ENTITY_ID = field("entity_id", String.class);
    private final static Field<Long> SEQUENCE_NUM = field("sequence_num", Long.class);
    private final static Field<String> EVENT_TYPE = field("event_type", String.class);
    private final static Field<Long> VERSION = field("version", Long.class);
    private final static Field<String> TRANSACTION_ID = field("transaction_id", String.class);
    private final static Field<JsonNode> EVENT = field("event", SQLDataType.JSONB.asConvertedDataType(new JsonBConverter()));
    private final static Field<JsonNode> METADATA = field("metadata", SQLDataType.JSONB.asConvertedDataType(new JsonBConverter()));
    private final static Field<JsonNode> CONTEXT = field("context", SQLDataType.JSONB.asConvertedDataType(new JsonBConverter()));
    private final static Field<Integer> TOTAL_MESSAGE_IN_TRANSACTION = field("total_message_in_transaction", Integer.class);
    private final static Field<Integer> NUM_MESSAGE_IN_TRANSACTION = field("num_message_in_transaction", Integer.class);
    private final static Field<String> USER_ID = field("user_id", String.class);
    private final static Field<String> SYSTEM_ID = field("system_id", String.class);
    private final static Field<LocalDateTime> EMISSION_DATE = field("emission_date", LocalDateTime.class);
    private final static Field<Boolean> PUBLISHED = field("published", Boolean.class);

    private final SimpleDb<Tx> simpleDb;
    private final TableNames tableNames;
    private final EventPublisher<E, Meta, Context> eventPublisher;
    private final JacksonEventFormat<?, E> eventFormat;
    private final JacksonSimpleFormat<Meta> metaFormat;
    private final JacksonSimpleFormat<Context> contextFormat;
    private final ObjectMapper objectMapper;

    public ReactivePostgresEventStore(
            EventPublisher<E, Meta, Context> eventPublisher,
            SimpleDb<Tx> simpleDb,
            TableNames tableNames, JacksonEventFormat<?, E> eventFormat,
            JacksonSimpleFormat<Meta> metaFormat,
            JacksonSimpleFormat<Context> contextFormat) {

        this.simpleDb = simpleDb;
        this.tableNames = tableNames;
        this.eventPublisher = eventPublisher;
        this.eventFormat = eventFormat;
        this.metaFormat = metaFormat;
        this.contextFormat = contextFormat;
        this.objectMapper = MapperSingleton.getInstance();
    }

    public static <E extends Event, Meta, Context> ReactivePostgresEventStore<PgAsyncTransaction, E, Meta, Context> create(
            EventPublisher<E, Meta, Context> eventPublisher,
            PgAsyncPool pgAsyncPool,
            TableNames tableNames,
            JacksonEventFormat<?, E> eventFormat,
            JacksonSimpleFormat<Meta> metaFormat,
            JacksonSimpleFormat<Context> contextFormat) {
        return new ReactivePostgresEventStore<>(eventPublisher, new SimpleDb<>() {
            @Override
            public CompletionStage<Integer> execute(Function<DSLContext, ? extends org.jooq.Query> queryFunction) {
                return pgAsyncPool.execute(queryFunction);
            }

            @Override
            public CompletionStage<PgAsyncTransaction> begin() {
                return pgAsyncPool.begin();
            }
        }, tableNames, eventFormat, metaFormat, contextFormat);
    }

    public static <E extends Event, Meta, Context> ReactivePostgresEventStore<fr.maif.jooq.reactor.PgAsyncTransaction, E, Meta, Context> create(
            EventPublisher<E, Meta, Context> eventPublisher,
            fr.maif.jooq.reactor.PgAsyncPool pgAsyncPool,
            TableNames tableNames,
            JacksonEventFormat<?, E> eventFormat,
            JacksonSimpleFormat<Meta> metaFormat,
            JacksonSimpleFormat<Context> contextFormat) {
        return new ReactivePostgresEventStore<>(eventPublisher, new SimpleDb<>() {
            @Override
            public CompletionStage<Integer> execute(Function<DSLContext, ? extends org.jooq.Query> queryFunction) {
                return pgAsyncPool.execute(queryFunction);
            }

            @Override
            public CompletionStage<fr.maif.jooq.reactor.PgAsyncTransaction> begin() {
                return pgAsyncPool.beginMono().toFuture();
            }
        }, tableNames, eventFormat, metaFormat, contextFormat);
    }

    @Override
    public CompletionStage<Tx> openTransaction() {
        return this.simpleDb.begin();
    }

    @Override
    public CompletionStage<Void> commitOrRollback(Option<Throwable> mayBeCrash, Tx pgAsyncTransaction) {
        return mayBeCrash.fold(
                pgAsyncTransaction::commit,
                e -> pgAsyncTransaction.rollback()
        );
    }

    @Override
    public CompletionStage<Void> persist(Tx transactionContext, List<EventEnvelope<E, Meta, Context>> events) {
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
                    JSONB eventString = Try.of(() -> JSONB.valueOf(objectMapper.writeValueAsString(eventFormat.write(event.event))))
                            .get();
                    JSONB contextString = contextFormat.write(Option.of(event.context))
                            .flatMap(c -> Try.of(() -> JSONB.valueOf(objectMapper.writeValueAsString(c))).toOption())
                            .getOrNull();
                    JSONB metaString = metaFormat.write(Option.of(event.metadata))
                            .flatMap(m -> Try.of(() -> JSONB.valueOf(objectMapper.writeValueAsString(m))).toOption())
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
        ).thenRun(() -> {});
    }

    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadEventsUnpublished(Tx transaction, ConcurrentReplayStrategy concurrentReplayStrategy) {
        return Flux.from(transaction.stream(500, dsl -> {
                    SelectSeekStep1<Record15<UUID, String, Long, String, Long, String, JsonNode, JsonNode, LocalDateTime, String, String, Integer, Integer, JsonNode, Boolean>, Long> tmpQuery = dsl
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
                                    PUBLISHED
                            )
                            .from(table(this.tableNames.tableName))
                            .where(PUBLISHED.isFalse())
                            .orderBy(SEQUENCE_NUM.asc());
                    switch (concurrentReplayStrategy) {
                        case WAIT:
                            return tmpQuery.forUpdate().of(table(this.tableNames.tableName));
                        case SKIP:
                            return tmpQuery.forUpdate().of(table(this.tableNames.tableName)).skipLocked();
                        default:
                            return tmpQuery;
                    }
            })).map(this::rsToEnvelope);
    }

    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadEventsByQuery(Tx tx, Query query) {

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

        return Flux.from(tx.stream(500, dsl -> {
            SelectSeekStep1<Record15<UUID, String, Long, String, Long, String, JsonNode, JsonNode, LocalDateTime, String, String, Integer, Integer, JsonNode, Boolean>, Long> queryBuilder = dsl
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
                    .orderBy(SEQUENCE_NUM);
            if (Objects.nonNull(query.size)) {
                return queryBuilder.limit(query.size);
            }
            return queryBuilder;
        })).map(this::rsToEnvelope);
    }

    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadEventsByQuery(Query query) {
        return Mono.fromCompletionStage(this.simpleDb::begin)
                .flatMapMany(ctx ->
                        Flux.from(loadEventsByQuery(ctx, query))
                                .doOnError(e -> {
                                    LOGGER.error("loadEventsByQuery terminated with error", e);
                                    ctx.rollback();
                                })
                                .doOnComplete(() -> {
                                    LOGGER.debug("loadEventsByQuery terminated correctly");
                                    ctx.commit();
                                })
                );
    }

    @Override
    public CompletionStage<Long> nextSequence(Tx tx) {
        return tx.queryOne(dsl ->
                dsl.resultQuery("select nextval('" + this.tableNames.sequenceNumName + "')")
        ).thenApply(mayBeResult -> mayBeResult.map(r -> r.get(0, Long.class)).getOrNull());
    }

    @Override
    public CompletionStage<Void> publish(List<EventEnvelope<E, Meta, Context>> events) {
        LOGGER.debug("Publishing event {}", events);
        return this.eventPublisher.publish(events);
    }

    @Override
    public CompletionStage<EventEnvelope<E, Meta, Context>> markAsPublished(EventEnvelope<E, Meta, Context> eventEnvelope) {
        return simpleDb.execute(dsl -> dsl
                .update(table(this.tableNames.tableName))
                .set(PUBLISHED, true)
                .where(ID.eq(eventEnvelope.id))
        ).thenApply(__ -> eventEnvelope.copy().withPublished(true).build());
    }

    @Override
    public CompletionStage<EventEnvelope<E, Meta, Context>> markAsPublished(Tx transaction, EventEnvelope<E, Meta, Context> eventEnvelope) {
        return transaction.execute(dsl -> dsl
                .update(table(this.tableNames.tableName))
                .set(PUBLISHED, true)
                .where(ID.eq(eventEnvelope.id))
        ).thenApply(__ -> eventEnvelope.copy().withPublished(true).build());
    }

    @Override
    public CompletionStage<List<EventEnvelope<E, Meta, Context>>> markAsPublished(Tx transaction, List<EventEnvelope<E, Meta, Context>> eventEnvelopes) {
        return transaction.execute(dsl -> dsl
                .update(table(this.tableNames.tableName))
                .set(PUBLISHED, true)
                .where(ID.in(eventEnvelopes.map(evt -> evt.id).toJavaArray(UUID[]::new)))
        ).thenApply(__ -> eventEnvelopes.map(eventEnvelope -> eventEnvelope.copy().withPublished(true).build()));
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

    private Option<JsonNode> readValue(JsonNode value) {
        return Option.of(value);
    }


    public static class JsonBConverter implements Converter<JSONB, JsonNode> {

        @Override
        public JsonNode from(JSONB databaseObject) {
            if (databaseObject != null && databaseObject.data() != null) {
                JsonNode parsed = Json.parse(databaseObject.data());
                if (parsed.isTextual()) {
                    return Json.parse(parsed.asText());
                }
                return parsed;
            } else {
                return NullNode.getInstance();
            }
        }

        @Override
        public JSONB to(JsonNode userObject) {
            if (userObject == null) {
                return null;
            }
            return JSONB.valueOf(Json.stringify(userObject));
        }

        @Override
        public Class<JSONB> fromType() {
            return JSONB.class;
        }

        @Override
        public Class<JsonNode> toType() {
            return JsonNode.class;
        }
    }

}
