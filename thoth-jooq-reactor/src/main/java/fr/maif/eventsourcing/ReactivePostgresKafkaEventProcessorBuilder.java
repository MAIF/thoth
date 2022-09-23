package fr.maif.eventsourcing;

import fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.eventsourcing.impl.DefaultAggregateStore;
import fr.maif.eventsourcing.impl.ReactorKafkaEventPublisher;
import fr.maif.jooq.PgAsyncPool;
import fr.maif.jooq.PgAsyncTransaction;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.control.Option;
import reactor.kafka.sender.SenderOptions;

import java.util.function.Function;

public class ReactivePostgresKafkaEventProcessorBuilder {


    public static class BuilderWithPool {
        public final PgAsyncPool pgAsyncPool;

        public BuilderWithPool(PgAsyncPool pgAsyncPool) {
            this.pgAsyncPool = pgAsyncPool;
        }

        public BuilderWithTables withTables(TableNames tableNames) {
            return new BuilderWithTables(pgAsyncPool, tableNames);
        }
    }

    public static class BuilderWithTables {
        public final PgAsyncPool pgAsyncPool;
        public final TableNames tableNames;

        public BuilderWithTables(PgAsyncPool pgAsyncPool, TableNames tableNames) {
            this.pgAsyncPool = pgAsyncPool;
            this.tableNames = tableNames;
        }

        public BuilderWithTx withExistingTransactionManager(TransactionManager<PgAsyncTransaction> transactionManager) {
            return new BuilderWithTx(pgAsyncPool, tableNames, transactionManager);
        }

        public BuilderWithTx withTransactionManager() {
            return new BuilderWithTx(pgAsyncPool, tableNames, new ReactiveTransactionManager(pgAsyncPool));
        }
    }

    public static class BuilderWithTx<Tx extends PgAsyncTransaction> {
        public final PgAsyncPool pgAsyncPool;
        public final TableNames tableNames;
        public final TransactionManager<PgAsyncTransaction> transactionManager;

        public BuilderWithTx(PgAsyncPool pgAsyncPool, TableNames tableNames,
                TransactionManager<PgAsyncTransaction> transactionManager) {
            this.pgAsyncPool = pgAsyncPool;
            this.tableNames = tableNames;
            this.transactionManager = transactionManager;
        }

        public <E extends Event> BuilderWithEventFormat<E> withEventFormater(JacksonEventFormat<?, E> eventFormat) {
            return new BuilderWithEventFormat<>(pgAsyncPool, tableNames, transactionManager, eventFormat);
        }
    }

    public static class BuilderWithEventFormat<E extends Event> {

        public final PgAsyncPool pgAsyncPool;
        public final TableNames tableNames;
        public final TransactionManager<PgAsyncTransaction> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;

        public BuilderWithEventFormat(PgAsyncPool pgAsyncPool, TableNames tableNames,
                TransactionManager<PgAsyncTransaction> transactionManager, JacksonEventFormat<?, E> eventFormat) {
            this.pgAsyncPool = pgAsyncPool;
            this.tableNames = tableNames;
            this.transactionManager = transactionManager;
            this.eventFormat = eventFormat;
        }

        public <Meta> BuilderWithMetaFormat<E, Meta> withMetaFormater(JacksonSimpleFormat<Meta> metaFormat) {
            return new BuilderWithMetaFormat<E, Meta>(pgAsyncPool, tableNames, transactionManager, eventFormat, metaFormat);
        }

        public BuilderWithMetaFormat<E, Tuple0> withNoMetaFormater() {
            return new BuilderWithMetaFormat<E, Tuple0>(pgAsyncPool, tableNames, transactionManager, eventFormat, JacksonSimpleFormat.<Tuple0>empty());
        }
    }

    public static class BuilderWithMetaFormat<E extends Event, Meta> {

        public final PgAsyncPool pgAsyncPool;
        public final TableNames tableNames;
        public final TransactionManager<PgAsyncTransaction> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final JacksonSimpleFormat<Meta> metaFormat;

        public BuilderWithMetaFormat(PgAsyncPool pgAsyncPool, TableNames tableNames,
                TransactionManager<PgAsyncTransaction> transactionManager, JacksonEventFormat<?, E> eventFormat,
                JacksonSimpleFormat<Meta> metaFormat) {

            this.pgAsyncPool = pgAsyncPool;
            this.tableNames = tableNames;
            this.transactionManager = transactionManager;
            this.eventFormat = eventFormat;
            this.metaFormat = metaFormat;
        }

        public <Context> BuilderWithContextFormat<E, Meta, Context> withContextFormater(JacksonSimpleFormat<Context> contextFormat) {
            return new BuilderWithContextFormat<E, Meta, Context>(pgAsyncPool, tableNames, transactionManager, eventFormat, metaFormat, contextFormat);
        }

        public BuilderWithContextFormat<E, Meta, Tuple0> withNoContextFormater() {
            return new BuilderWithContextFormat<E, Meta, Tuple0>(pgAsyncPool, tableNames, transactionManager, eventFormat, metaFormat, JacksonSimpleFormat.<Tuple0>empty());
        }
    }

    public static class BuilderWithContextFormat<E extends Event, Meta, Context> {

        public final PgAsyncPool pgAsyncPool;
        public final TableNames tableNames;
        public final TransactionManager<PgAsyncTransaction> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final JacksonSimpleFormat<Meta> metaFormat;
        public final JacksonSimpleFormat<Context> contextFormat;

        public BuilderWithContextFormat(PgAsyncPool pgAsyncPool, TableNames tableNames,
                TransactionManager<PgAsyncTransaction> transactionManager, JacksonEventFormat<?, E> eventFormat,
                JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat) {

            this.pgAsyncPool = pgAsyncPool;
            this.tableNames = tableNames;
            this.transactionManager = transactionManager;
            this.eventFormat = eventFormat;
            this.metaFormat = metaFormat;
            this.contextFormat = contextFormat;
        }

        public BuilderWithKafkaSettings<E, Meta, Context> withKafkaSettings(String topic, SenderOptions<String, EventEnvelope<E, Meta, Context>> producerSettings, Integer bufferSize) {
            return new BuilderWithKafkaSettings<E, Meta, Context>(
                    pgAsyncPool,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    topic,
                    producerSettings,
                    bufferSize,
                    null
            );
        }

        public BuilderWithKafkaSettings<E, Meta, Context> withKafkaSettings(String topic, SenderOptions<String, EventEnvelope<E, Meta, Context>> producerSettings) {
            return withKafkaSettings(topic, producerSettings, 1000);
        }
    }


    public static class BuilderWithKafkaSettings<E extends Event, Meta, Context> {

        public final PgAsyncPool pgAsyncPool;
        public final TableNames tableNames;
        public final TransactionManager<PgAsyncTransaction> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final JacksonSimpleFormat<Meta> metaFormat;
        public final JacksonSimpleFormat<Context> contextFormat;
        public final ConcurrentReplayStrategy concurrentReplayStrategy;
        public final ReactorKafkaEventPublisher<E, Meta, Context> eventPublisher;
        public final ReactivePostgresEventStore<PgAsyncTransaction, E, Meta, Context> eventStore;

        private BuilderWithKafkaSettings(PgAsyncPool pgAsyncPool, TableNames tableNames, TransactionManager<PgAsyncTransaction> transactionManager, JacksonEventFormat<?, E> eventFormat, JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat, ConcurrentReplayStrategy concurrentReplayStrategy, ReactorKafkaEventPublisher<E, Meta, Context> eventPublisher, ReactivePostgresEventStore<PgAsyncTransaction, E, Meta, Context> eventStore) {

            this.pgAsyncPool = pgAsyncPool;
            this.tableNames = tableNames;
            this.transactionManager = transactionManager;
            this.eventFormat = eventFormat;
            this.metaFormat = metaFormat;
            this.contextFormat = contextFormat;
            this.concurrentReplayStrategy = concurrentReplayStrategy;
            this.eventPublisher = eventPublisher;
            this.eventStore = eventStore;
        }

        BuilderWithKafkaSettings(PgAsyncPool pgAsyncPool, TableNames tableNames, TransactionManager<PgAsyncTransaction> transactionManager, JacksonEventFormat<?, E> eventFormat, JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat, String topic, SenderOptions<String, EventEnvelope<E, Meta, Context>> producerSettings, Integer bufferSize, ConcurrentReplayStrategy concurrentReplayStrategy) {

            this.pgAsyncPool = pgAsyncPool;
            this.tableNames = tableNames;
            this.transactionManager = transactionManager;
            this.eventFormat = eventFormat;
            this.metaFormat = metaFormat;
            this.contextFormat = contextFormat;
            this.eventPublisher = new ReactorKafkaEventPublisher<E, Meta, Context>(producerSettings, topic, bufferSize);
            this.concurrentReplayStrategy = Option.of(concurrentReplayStrategy).getOrElse(ConcurrentReplayStrategy.WAIT);
            this.eventStore = ReactivePostgresEventStore.create(
                    eventPublisher,
                    pgAsyncPool,
                    tableNames,
                    eventFormat,
                    metaFormat,
                    contextFormat
            );
        }

        public BuilderWithKafkaSettings<E, Meta, Context> withSkipConcurrentReplayStrategy() {
            return new BuilderWithKafkaSettings<>(
                    pgAsyncPool,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    ConcurrentReplayStrategy.SKIP,
                    eventPublisher,
                    eventStore
            );
        }


        public BuilderWithKafkaSettings<E, Meta, Context> withNoConcurrentReplayStrategy() {
            return new BuilderWithKafkaSettings<>(
                    pgAsyncPool,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    ConcurrentReplayStrategy.NO_STRATEGY,
                    eventPublisher,
                    eventStore
            );
        }

        public BuilderWithKafkaSettings<E, Meta, Context> withWaitConcurrentReplayStrategy() {
            return new BuilderWithKafkaSettings<>(
                    pgAsyncPool,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    ConcurrentReplayStrategy.WAIT,
                    eventPublisher,
                    eventStore
            );
        }

        public BuilderWithKafkaSettings<E, Meta, Context> withConcurrentReplayStrategy(ConcurrentReplayStrategy concurrentReplayStrategy) {
            return new BuilderWithKafkaSettings<>(
                    pgAsyncPool,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    concurrentReplayStrategy,
                    eventPublisher,
                    eventStore
            );
        }

        public <S extends State<S>> BuilderWithEventHandler<S, E, Meta, Context> withEventHandler(EventHandler<S, E> eventHandler) {
            return new BuilderWithEventHandler<>(
                    pgAsyncPool,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    eventPublisher,
                    concurrentReplayStrategy,
                    eventStore,
                    eventHandler
            );
        }
    }

        public static class BuilderWithEventHandler<S extends State<S>, E extends Event, Meta, Context> {

            public final PgAsyncPool pgAsyncPool;
            public final TableNames tableNames;
            public final TransactionManager<PgAsyncTransaction> transactionManager;
            public final JacksonEventFormat<?, E> eventFormat;
            public final JacksonSimpleFormat<Meta> metaFormat;
            public final JacksonSimpleFormat<Context> contextFormat;
            public final ReactorKafkaEventPublisher<E, Meta, Context> eventPublisher;
            public final ConcurrentReplayStrategy concurrentReplayStrategy;
            public final ReactivePostgresEventStore<PgAsyncTransaction, E, Meta, Context> eventStore;
            public final EventHandler<S, E> eventHandler;

            public BuilderWithEventHandler(PgAsyncPool pgAsyncPool, TableNames tableNames,
                                           TransactionManager<PgAsyncTransaction> transactionManager, JacksonEventFormat<?, E> eventFormat,
                                           JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat,
                                           ReactorKafkaEventPublisher<E, Meta, Context> eventPublisher, ConcurrentReplayStrategy concurrentReplayStrategy,
                                           ReactivePostgresEventStore<PgAsyncTransaction, E, Meta, Context> eventStore, EventHandler<S, E> eventHandler) {

                this.pgAsyncPool = pgAsyncPool;
                this.tableNames = tableNames;
                this.transactionManager = transactionManager;
                this.eventFormat = eventFormat;
                this.metaFormat = metaFormat;
                this.contextFormat = contextFormat;
                this.eventPublisher = eventPublisher;
                this.concurrentReplayStrategy = concurrentReplayStrategy;
                this.eventStore = eventStore;
                this.eventHandler = eventHandler;
            }

            public BuilderWithAggregateStore<S, E, Meta, Context> withAggregateStore(Function<BuilderWithEventHandler<S, E, Meta, Context>, AggregateStore<S, String, PgAsyncTransaction>> builder) {
            return new BuilderWithAggregateStore<>(
                    pgAsyncPool,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    eventPublisher,
                    concurrentReplayStrategy,
                    eventStore,
                    eventHandler,
                    builder.apply(this));
        }

        public BuilderWithAggregateStore<S, E, Meta, Context> withAggregateStore(AggregateStore<S, String, PgAsyncTransaction> aggregateStore) {
            return new BuilderWithAggregateStore<>(
                    pgAsyncPool,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    eventPublisher,
                    concurrentReplayStrategy,
                    eventStore,
                    eventHandler,
                    aggregateStore);
        }

        public BuilderWithAggregateStore<S, E, Meta, Context> withDefaultAggregateStore() {
            return new BuilderWithAggregateStore<>(
                    pgAsyncPool,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    eventPublisher,
                    concurrentReplayStrategy,
                    eventStore,
                    eventHandler,
                    new DefaultAggregateStore<>(eventStore, eventHandler, transactionManager));
        }
    }


    public static class BuilderWithAggregateStore<S extends State<S>, E extends Event, Meta, Context> {

        public final PgAsyncPool pgAsyncPool;
        public final TableNames tableNames;
        public final TransactionManager<PgAsyncTransaction> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final JacksonSimpleFormat<Meta> metaFormat;
        public final JacksonSimpleFormat<Context> contextFormat;
        public final ReactorKafkaEventPublisher<E, Meta, Context> eventPublisher;
        public final ConcurrentReplayStrategy concurrentReplayStrategy;
        public final ReactivePostgresEventStore<PgAsyncTransaction, E, Meta, Context> eventStore;
        public final EventHandler<S, E> eventHandler;
        public final AggregateStore<S, String, PgAsyncTransaction> aggregateStore;

        public BuilderWithAggregateStore(PgAsyncPool pgAsyncPool, TableNames tableNames,
                                         TransactionManager<PgAsyncTransaction> transactionManager, JacksonEventFormat<?, E> eventFormat,
                                         JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat,
                                         ReactorKafkaEventPublisher<E, Meta, Context> eventPublisher, ConcurrentReplayStrategy concurrentReplayStrategy,
                                         ReactivePostgresEventStore<PgAsyncTransaction, E, Meta, Context> eventStore, EventHandler<S, E> eventHandler,
                                         AggregateStore<S, String, PgAsyncTransaction> aggregateStore) {

            this.pgAsyncPool = pgAsyncPool;
            this.tableNames = tableNames;
            this.transactionManager = transactionManager;
            this.eventFormat = eventFormat;
            this.metaFormat = metaFormat;
            this.contextFormat = contextFormat;
            this.eventPublisher = eventPublisher;
            this.concurrentReplayStrategy = concurrentReplayStrategy;
            this.eventStore = eventStore;
            this.eventHandler = eventHandler;
            this.aggregateStore = aggregateStore;
        }

        public <Error, C extends Command<Meta, Context>, Message> BuilderWithCommandHandler<Error, S, C, E, Message, Meta, Context> withCommandHandler(CommandHandlerGetter<Error, S, C, E, Message, PgAsyncTransaction> commandHandler) {
            return new BuilderWithCommandHandler<>(
                    pgAsyncPool,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    eventPublisher,
                    concurrentReplayStrategy,
                    eventStore,
                    aggregateStore,
                    eventHandler,
                    commandHandler.commandHandler()
            );
        }

        public <Error, C extends Command<Meta, Context>, Message> BuilderWithCommandHandler<Error, S, C, E, Message, Meta, Context> withCommandHandler(Function<BuilderWithAggregateStore<S, E, Meta, Context>, CommandHandlerGetter<Error, S, C, E, Message, PgAsyncTransaction>> commandHandler) {
            return new BuilderWithCommandHandler<>(
                    pgAsyncPool,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    eventPublisher,
                    concurrentReplayStrategy,
                    eventStore,
                    aggregateStore,
                    eventHandler,
                    commandHandler.apply(this).commandHandler()
            );
        }
    }

    public static class BuilderWithCommandHandler<Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, Message, Meta, Context> {

        public final PgAsyncPool pgAsyncPool;
        public final TableNames tableNames;
        public final TransactionManager<PgAsyncTransaction> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final JacksonSimpleFormat<Meta> metaFormat;
        public final JacksonSimpleFormat<Context> contextFormat;
        public final ReactorKafkaEventPublisher<E, Meta, Context> eventPublisher;
        public final ConcurrentReplayStrategy concurrentReplayStrategy;
        public final ReactivePostgresEventStore<PgAsyncTransaction, E, Meta, Context> eventStore;
        public final AggregateStore<S, String, PgAsyncTransaction> aggregateStore;
        public final EventHandler<S, E> eventHandler;
        public final CommandHandler<Error, S, C, E, Message, PgAsyncTransaction> commandHandler;

        public BuilderWithCommandHandler(PgAsyncPool pgAsyncPool, TableNames tableNames,
                                         TransactionManager<PgAsyncTransaction> transactionManager, JacksonEventFormat<?, E> eventFormat,
                                         JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat,
                                         ReactorKafkaEventPublisher<E, Meta, Context> eventPublisher, ConcurrentReplayStrategy concurrentReplayStrategy,
                                         ReactivePostgresEventStore<PgAsyncTransaction, E, Meta, Context> eventStore,
                                         AggregateStore<S, String, PgAsyncTransaction> aggregateStore, EventHandler<S, E> eventHandler,
                                         CommandHandler<Error, S, C, E, Message, PgAsyncTransaction> commandHandler) {

            this.pgAsyncPool = pgAsyncPool;
            this.tableNames = tableNames;
            this.transactionManager = transactionManager;
            this.eventFormat = eventFormat;
            this.metaFormat = metaFormat;
            this.contextFormat = contextFormat;
            this.eventPublisher = eventPublisher;
            this.concurrentReplayStrategy = concurrentReplayStrategy;
            this.eventStore = eventStore;
            this.aggregateStore = aggregateStore;
            this.eventHandler = eventHandler;
            this.commandHandler = commandHandler;
        }

        public BuilderWithProjections<Error, S, C, E, Message, Meta, Context> withProjections(List<? extends ProjectionGetter<PgAsyncTransaction, E, Meta, Context>> projections) {
            return new BuilderWithProjections<>(
                    pgAsyncPool,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    eventPublisher,
                    concurrentReplayStrategy,
                    eventStore,
                    aggregateStore,
                    eventHandler,
                    commandHandler,
                    projections.map(ProjectionGetter::projection)
            );
        }

        public BuilderWithProjections<Error, S, C, E, Message, Meta, Context> withProjections(Function<BuilderWithCommandHandler<Error, S, C, E, Message, Meta, Context>, List<? extends ProjectionGetter<PgAsyncTransaction, E, Meta, Context>>> projections) {
            return new BuilderWithProjections<>(
                    pgAsyncPool,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    eventPublisher,
                    concurrentReplayStrategy,
                    eventStore,
                    aggregateStore,
                    eventHandler,
                    commandHandler,
                    projections.apply(this).map(ProjectionGetter::projection)
            );
        }

        public BuilderWithProjections<Error, S, C, E, Message, Meta, Context> withProjections(ProjectionGetter<PgAsyncTransaction, E, Meta, Context>... projections) {
            return withProjections(List.of(projections));
        }

        public BuilderWithProjections<Error, S, C, E, Message, Meta, Context> withNoProjections() {
            return withProjections(List.empty());
        }
    }

    public static class BuilderWithProjections<Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, Message, Meta, Context> {

        public final PgAsyncPool pgAsyncPool;
        public final TableNames tableNames;
        public final TransactionManager<PgAsyncTransaction> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final JacksonSimpleFormat<Meta> metaFormat;
        public final JacksonSimpleFormat<Context> contextFormat;
        public final ReactorKafkaEventPublisher<E, Meta, Context> eventPublisher;
        public final ConcurrentReplayStrategy concurrentReplayStrategy;
        public final ReactivePostgresEventStore<PgAsyncTransaction, E, Meta, Context> eventStore;
        public final AggregateStore<S, String, PgAsyncTransaction> aggregateStore;
        public final EventHandler<S, E> eventHandler;
        public final CommandHandler<Error, S, C, E, Message, PgAsyncTransaction> commandHandler;
        public final List<Projection<PgAsyncTransaction, E, Meta, Context>> projections;

        public BuilderWithProjections(PgAsyncPool pgAsyncPool, TableNames tableNames,
                                      TransactionManager<PgAsyncTransaction> transactionManager, JacksonEventFormat<?, E> eventFormat,
                                      JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat,
                                      ReactorKafkaEventPublisher<E, Meta, Context> eventPublisher, ConcurrentReplayStrategy concurrentReplayStrategy,
                                      ReactivePostgresEventStore<PgAsyncTransaction, E, Meta, Context> eventStore,
                                      AggregateStore<S, String, PgAsyncTransaction> aggregateStore, EventHandler<S, E> eventHandler,
                                      CommandHandler<Error, S, C, E, Message, PgAsyncTransaction> commandHandler,
                                      List<Projection<PgAsyncTransaction, E, Meta, Context>> projections) {

            this.pgAsyncPool = pgAsyncPool;
            this.tableNames = tableNames;
            this.transactionManager = transactionManager;
            this.eventFormat = eventFormat;
            this.metaFormat = metaFormat;
            this.contextFormat = contextFormat;
            this.eventPublisher = eventPublisher;
            this.concurrentReplayStrategy = concurrentReplayStrategy;
            this.eventStore = eventStore;
            this.aggregateStore = aggregateStore;
            this.eventHandler = eventHandler;
            this.commandHandler = commandHandler;
            this.projections = projections;
        }

        public EventProcessor<Error, S, C, E, PgAsyncTransaction, Message, Meta, Context> build() {
            var ES = new EventProcessorImpl<Error, S, C, E, PgAsyncTransaction, Message, Meta, Context>(
                    eventStore,
                    transactionManager,
                    aggregateStore,
                    commandHandler,
                    eventHandler,
                    projections
            );
            eventPublisher.start(eventStore, concurrentReplayStrategy);
            return ES;
        }

    }

}
