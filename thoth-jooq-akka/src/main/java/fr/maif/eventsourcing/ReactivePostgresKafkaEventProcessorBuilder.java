package fr.maif.eventsourcing;

import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.eventsourcing.impl.DefaultAggregateStore;
import fr.maif.eventsourcing.impl.KafkaEventPublisher;
import fr.maif.jooq.PgAsyncPool;
import fr.maif.jooq.PgAsyncTransaction;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.control.Option;

import java.util.function.Function;

public class ReactivePostgresKafkaEventProcessorBuilder {


    public static class BuilderWithSystem {
        public final ActorSystem system;

        public BuilderWithSystem(ActorSystem system) {
            this.system = system;
        }

        public BuilderWithPool withPgAsyncPool(PgAsyncPool pgAsyncPool) {
            return new BuilderWithPool(system, pgAsyncPool);
        }
    }

    public static class BuilderWithPool {
        public final ActorSystem system;
        public final PgAsyncPool pgAsyncPool;

        public BuilderWithPool(ActorSystem system, PgAsyncPool pgAsyncPool) {
            this.system = system;
            this.pgAsyncPool = pgAsyncPool;
        }

        public BuilderWithTables withTables(TableNames tableNames) {
            return new BuilderWithTables(system, pgAsyncPool, tableNames);
        }
    }

    public static class BuilderWithTables {
        public final ActorSystem system;
        public final PgAsyncPool pgAsyncPool;
        public final TableNames tableNames;

        public BuilderWithTables(ActorSystem system, PgAsyncPool pgAsyncPool, TableNames tableNames) {
            this.system = system;
            this.pgAsyncPool = pgAsyncPool;
            this.tableNames = tableNames;
        }

        public BuilderWithTx withExistingTransactionManager(TransactionManager<PgAsyncTransaction> transactionManager) {
            return new BuilderWithTx(system, pgAsyncPool, tableNames, transactionManager);
        }

        public BuilderWithTx withTransactionManager() {
            return new BuilderWithTx(system, pgAsyncPool, tableNames, new ReactiveTransactionManager(pgAsyncPool));
        }
    }

    public static class BuilderWithTx {
        public final ActorSystem system;
        public final PgAsyncPool pgAsyncPool;
        public final TableNames tableNames;
        public final TransactionManager<PgAsyncTransaction> transactionManager;

        public BuilderWithTx(ActorSystem system, PgAsyncPool pgAsyncPool, TableNames tableNames,
                TransactionManager<PgAsyncTransaction> transactionManager) {
            this.system = system;
            this.pgAsyncPool = pgAsyncPool;
            this.tableNames = tableNames;
            this.transactionManager = transactionManager;
        }

        public <E extends Event> BuilderWithEventFormat<E> withEventFormater(JacksonEventFormat<?, E> eventFormat) {
            return new BuilderWithEventFormat<>(system, pgAsyncPool, tableNames, transactionManager, eventFormat);
        }
    }

    public static class BuilderWithEventFormat<E extends Event> {
        public final ActorSystem system;
        public final PgAsyncPool pgAsyncPool;
        public final TableNames tableNames;
        public final TransactionManager<PgAsyncTransaction> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;

        public BuilderWithEventFormat(ActorSystem system, PgAsyncPool pgAsyncPool, TableNames tableNames,
                TransactionManager<PgAsyncTransaction> transactionManager, JacksonEventFormat<?, E> eventFormat) {
            this.system = system;
            this.pgAsyncPool = pgAsyncPool;
            this.tableNames = tableNames;
            this.transactionManager = transactionManager;
            this.eventFormat = eventFormat;
        }

        public <Meta> BuilderWithMetaFormat<E, Meta> withMetaFormater(JacksonSimpleFormat<Meta> metaFormat) {
            return new BuilderWithMetaFormat<E, Meta>(system, pgAsyncPool, tableNames, transactionManager, eventFormat, metaFormat);
        }

        public BuilderWithMetaFormat<E, Tuple0> withNoMetaFormater() {
            return new BuilderWithMetaFormat<E, Tuple0>(system, pgAsyncPool, tableNames, transactionManager, eventFormat, JacksonSimpleFormat.<Tuple0>empty());
        }
    }

    public static class BuilderWithMetaFormat<E extends Event, Meta> {
        public final ActorSystem system;
        public final PgAsyncPool pgAsyncPool;
        public final TableNames tableNames;
        public final TransactionManager<PgAsyncTransaction> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final JacksonSimpleFormat<Meta> metaFormat;

        public BuilderWithMetaFormat(ActorSystem system, PgAsyncPool pgAsyncPool, TableNames tableNames,
                TransactionManager<PgAsyncTransaction> transactionManager, JacksonEventFormat<?, E> eventFormat,
                JacksonSimpleFormat<Meta> metaFormat) {
            this.system = system;
            this.pgAsyncPool = pgAsyncPool;
            this.tableNames = tableNames;
            this.transactionManager = transactionManager;
            this.eventFormat = eventFormat;
            this.metaFormat = metaFormat;
        }

        public <Context> BuilderWithContextFormat<E, Meta, Context> withContextFormater(JacksonSimpleFormat<Context> contextFormat) {
            return new BuilderWithContextFormat<E, Meta, Context>(system, pgAsyncPool, tableNames, transactionManager, eventFormat, metaFormat, contextFormat);
        }

        public BuilderWithContextFormat<E, Meta, Tuple0> withNoContextFormater() {
            return new BuilderWithContextFormat<E, Meta, Tuple0>(system, pgAsyncPool, tableNames, transactionManager, eventFormat, metaFormat, JacksonSimpleFormat.<Tuple0>empty());
        }
    }

    public static class BuilderWithContextFormat<E extends Event, Meta, Context> {
        public final ActorSystem system;
        public final PgAsyncPool pgAsyncPool;
        public final TableNames tableNames;
        public final TransactionManager<PgAsyncTransaction> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final JacksonSimpleFormat<Meta> metaFormat;
        public final JacksonSimpleFormat<Context> contextFormat;

        public BuilderWithContextFormat(ActorSystem system, PgAsyncPool pgAsyncPool, TableNames tableNames,
                TransactionManager<PgAsyncTransaction> transactionManager, JacksonEventFormat<?, E> eventFormat,
                JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat) {
            this.system = system;
            this.pgAsyncPool = pgAsyncPool;
            this.tableNames = tableNames;
            this.transactionManager = transactionManager;
            this.eventFormat = eventFormat;
            this.metaFormat = metaFormat;
            this.contextFormat = contextFormat;
        }

        public BuilderWithKafkaSettings<E, Meta, Context> withKafkaSettings(String topic, ProducerSettings<String, EventEnvelope<E, Meta, Context>> producerSettings, Integer bufferSize) {
            return new BuilderWithKafkaSettings<>(
                    system,
                    pgAsyncPool,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    topic,
                    producerSettings,
                    bufferSize,
                    null);
        }

        public BuilderWithKafkaSettings<E, Meta, Context> withKafkaSettings(String topic, ProducerSettings<String, EventEnvelope<E, Meta, Context>> producerSettings) {
            return withKafkaSettings(topic, producerSettings, 1000);
        }
    }


    public static class BuilderWithKafkaSettings<E extends Event, Meta, Context> {
        public final ActorSystem system;
        public final PgAsyncPool pgAsyncPool;
        public final TableNames tableNames;
        public final TransactionManager<PgAsyncTransaction> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final JacksonSimpleFormat<Meta> metaFormat;
        public final JacksonSimpleFormat<Context> contextFormat;
        public final ConcurrentReplayStrategy concurrentReplayStrategy;
        public final KafkaEventPublisher<E, Meta, Context> eventPublisher;
        public final ReactivePostgresEventStore<E, Meta, Context> eventStore;

        private BuilderWithKafkaSettings(ActorSystem system, PgAsyncPool pgAsyncPool, TableNames tableNames, TransactionManager<PgAsyncTransaction> transactionManager, JacksonEventFormat<?, E> eventFormat, JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat, ConcurrentReplayStrategy concurrentReplayStrategy, KafkaEventPublisher<E, Meta, Context> eventPublisher, ReactivePostgresEventStore<E, Meta, Context> eventStore) {
            this.system = system;
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

        BuilderWithKafkaSettings(ActorSystem system, PgAsyncPool pgAsyncPool, TableNames tableNames, TransactionManager<PgAsyncTransaction> transactionManager, JacksonEventFormat<?, E> eventFormat, JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat, String topic, ProducerSettings<String, EventEnvelope<E, Meta, Context>> producerSettings, Integer bufferSize, ConcurrentReplayStrategy concurrentReplayStrategy) {
            this.system = system;
            this.pgAsyncPool = pgAsyncPool;
            this.tableNames = tableNames;
            this.transactionManager = transactionManager;
            this.eventFormat = eventFormat;
            this.metaFormat = metaFormat;
            this.contextFormat = contextFormat;
            this.eventPublisher = new KafkaEventPublisher<>(system, producerSettings, topic, bufferSize);
            this.concurrentReplayStrategy = Option.of(concurrentReplayStrategy).getOrElse(ConcurrentReplayStrategy.WAIT);
            this.eventStore = new ReactivePostgresEventStore<>(
                    system,
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
                    system,
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
                    system,
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
                    system,
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
                    system,
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
                    system,
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
            public final ActorSystem system;
            public final PgAsyncPool pgAsyncPool;
            public final TableNames tableNames;
            public final TransactionManager<PgAsyncTransaction> transactionManager;
            public final JacksonEventFormat<?, E> eventFormat;
            public final JacksonSimpleFormat<Meta> metaFormat;
            public final JacksonSimpleFormat<Context> contextFormat;
            public final KafkaEventPublisher<E, Meta, Context> eventPublisher;
            public final ConcurrentReplayStrategy concurrentReplayStrategy;
            public final ReactivePostgresEventStore<E, Meta, Context> eventStore;
            public final EventHandler<S, E> eventHandler;

            public BuilderWithEventHandler(ActorSystem system, PgAsyncPool pgAsyncPool, TableNames tableNames,
                    TransactionManager<PgAsyncTransaction> transactionManager, JacksonEventFormat<?, E> eventFormat,
                    JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat,
                    KafkaEventPublisher<E, Meta, Context> eventPublisher, ConcurrentReplayStrategy concurrentReplayStrategy,
                    ReactivePostgresEventStore<E, Meta, Context> eventStore, EventHandler<S, E> eventHandler) {
                this.system = system;
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
                    system,
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
                    system,
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
                    system,
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
                    new DefaultAggregateStore<>(eventStore, eventHandler, system, transactionManager));
        }
    }


    public static class BuilderWithAggregateStore<S extends State<S>, E extends Event, Meta, Context> {
        public final ActorSystem system;
        public final PgAsyncPool pgAsyncPool;
        public final TableNames tableNames;
        public final TransactionManager<PgAsyncTransaction> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final JacksonSimpleFormat<Meta> metaFormat;
        public final JacksonSimpleFormat<Context> contextFormat;
        public final KafkaEventPublisher<E, Meta, Context> eventPublisher;
        public final ConcurrentReplayStrategy concurrentReplayStrategy;
        public final ReactivePostgresEventStore<E, Meta, Context> eventStore;
        public final EventHandler<S, E> eventHandler;
        public final AggregateStore<S, String, PgAsyncTransaction> aggregateStore;

        public BuilderWithAggregateStore(ActorSystem system, PgAsyncPool pgAsyncPool, TableNames tableNames,
                TransactionManager<PgAsyncTransaction> transactionManager, JacksonEventFormat<?, E> eventFormat,
                JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat,
                KafkaEventPublisher<E, Meta, Context> eventPublisher, ConcurrentReplayStrategy concurrentReplayStrategy,
                ReactivePostgresEventStore<E, Meta, Context> eventStore, EventHandler<S, E> eventHandler,
                AggregateStore<S, String, PgAsyncTransaction> aggregateStore) {
            this.system = system;
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

        public <Error, C extends Command<Meta, Context>, Message> BuilderWithCommandHandler<Error, S, C, E, Message, Meta, Context> withCommandHandler(CommandHandler<Error, S, C, E, Message, PgAsyncTransaction> commandHandler) {
            return new BuilderWithCommandHandler<>(
                    system,
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
                    commandHandler
            );
        }

        public <Error, C extends Command<Meta, Context>, Message> BuilderWithCommandHandler<Error, S, C, E, Message, Meta, Context> withCommandHandler(Function<BuilderWithAggregateStore<S, E, Meta, Context>, CommandHandler<Error, S, C, E, Message, PgAsyncTransaction>> commandHandler) {
            return new BuilderWithCommandHandler<>(
                    system,
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
                    commandHandler.apply(this)
            );
        }
    }

    public static class BuilderWithCommandHandler<Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, Message, Meta, Context> {
        public final ActorSystem system;
        public final PgAsyncPool pgAsyncPool;
        public final TableNames tableNames;
        public final TransactionManager<PgAsyncTransaction> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final JacksonSimpleFormat<Meta> metaFormat;
        public final JacksonSimpleFormat<Context> contextFormat;
        public final KafkaEventPublisher<E, Meta, Context> eventPublisher;
        public final ConcurrentReplayStrategy concurrentReplayStrategy;
        public final ReactivePostgresEventStore<E, Meta, Context> eventStore;
        public final AggregateStore<S, String, PgAsyncTransaction> aggregateStore;
        public final EventHandler<S, E> eventHandler;
        public final CommandHandler<Error, S, C, E, Message, PgAsyncTransaction> commandHandler;

        public BuilderWithCommandHandler(ActorSystem system, PgAsyncPool pgAsyncPool, TableNames tableNames,
                TransactionManager<PgAsyncTransaction> transactionManager, JacksonEventFormat<?, E> eventFormat,
                JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat,
                KafkaEventPublisher<E, Meta, Context> eventPublisher, ConcurrentReplayStrategy concurrentReplayStrategy,
                ReactivePostgresEventStore<E, Meta, Context> eventStore,
                AggregateStore<S, String, PgAsyncTransaction> aggregateStore, EventHandler<S, E> eventHandler,
                CommandHandler<Error, S, C, E, Message, PgAsyncTransaction> commandHandler) {
            this.system = system;
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

        public BuilderWithProjections<Error, S, C, E, Message, Meta, Context> withProjections(List<Projection<PgAsyncTransaction, E, Meta, Context>> projections) {
            return new BuilderWithProjections<>(
                    system,
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
                    projections
            );
        }

        public BuilderWithProjections<Error, S, C, E, Message, Meta, Context> withProjections(Function<BuilderWithCommandHandler<Error, S, C, E, Message, Meta, Context>, List<Projection<PgAsyncTransaction, E, Meta, Context>>> projections) {
            return new BuilderWithProjections<>(
                    system,
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
                    projections.apply(this)
            );
        }

        public BuilderWithProjections<Error, S, C, E, Message, Meta, Context> withProjections(Projection<PgAsyncTransaction, E, Meta, Context>... projections) {
            return withProjections(List.of(projections));
        }

        public BuilderWithProjections<Error, S, C, E, Message, Meta, Context> withNoProjections() {
            return withProjections(List.empty());
        }
    }

    public static class BuilderWithProjections<Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, Message, Meta, Context> {
        public final ActorSystem system;
        public final PgAsyncPool pgAsyncPool;
        public final TableNames tableNames;
        public final TransactionManager<PgAsyncTransaction> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final JacksonSimpleFormat<Meta> metaFormat;
        public final JacksonSimpleFormat<Context> contextFormat;
        public final KafkaEventPublisher<E, Meta, Context> eventPublisher;
        public final ConcurrentReplayStrategy concurrentReplayStrategy;
        public final ReactivePostgresEventStore<E, Meta, Context> eventStore;
        public final AggregateStore<S, String, PgAsyncTransaction> aggregateStore;
        public final EventHandler<S, E> eventHandler;
        public final CommandHandler<Error, S, C, E, Message, PgAsyncTransaction> commandHandler;
        public final List<Projection<PgAsyncTransaction, E, Meta, Context>> projections;

        public BuilderWithProjections(ActorSystem system, PgAsyncPool pgAsyncPool, TableNames tableNames,
                TransactionManager<PgAsyncTransaction> transactionManager, JacksonEventFormat<?, E> eventFormat,
                JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat,
                KafkaEventPublisher<E, Meta, Context> eventPublisher, ConcurrentReplayStrategy concurrentReplayStrategy,
                ReactivePostgresEventStore<E, Meta, Context> eventStore,
                AggregateStore<S, String, PgAsyncTransaction> aggregateStore, EventHandler<S, E> eventHandler,
                CommandHandler<Error, S, C, E, Message, PgAsyncTransaction> commandHandler,
                List<Projection<PgAsyncTransaction, E, Meta, Context>> projections) {
            this.system = system;
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

        public ReactivePostgresKafkaEventProcessor<Error, S, C, E, Message, Meta, Context> build() {
            return new ReactivePostgresKafkaEventProcessor<Error, S, C, E, Message, Meta, Context>(
                    new ReactivePostgresKafkaEventProcessor.PostgresKafkaEventProcessorConfig<Error, S, C, E, Message, Meta, Context>(
                            concurrentReplayStrategy,
                            eventStore,
                            transactionManager,
                            aggregateStore,
                            commandHandler,
                            eventHandler,
                            projections,
                            eventPublisher
                    )
            );
        }

    }

}
