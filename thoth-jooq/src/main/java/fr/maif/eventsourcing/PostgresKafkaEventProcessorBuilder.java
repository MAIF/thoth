package fr.maif.eventsourcing;

import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.eventsourcing.impl.DefaultAggregateStore;
import fr.maif.eventsourcing.impl.JdbcTransactionManager;
import fr.maif.eventsourcing.impl.KafkaEventPublisher;
import fr.maif.eventsourcing.impl.PostgresEventStore;
import fr.maif.eventsourcing.impl.PostgresLockManager;
import fr.maif.eventsourcing.impl.TableNames;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.control.Option;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy.WAIT;

public class PostgresKafkaEventProcessorBuilder {



    public static class BuilderWithSystem {
        public final ActorSystem system;

        public BuilderWithSystem(ActorSystem system) {
            this.system = system;
        }

        public BuilderWithPool withDataSource(DataSource dataSource) {
            return new BuilderWithPool(system, dataSource);
        }
    }

    public static class BuilderWithPool {
        public final ActorSystem system;
        public final DataSource dataSource;

        public BuilderWithPool(ActorSystem system, DataSource dataSource) {
            this.system = system;
            this.dataSource = dataSource;
        }

        public BuilderWithTables withTables(TableNames tableNames) {
            return new BuilderWithTables(system, dataSource, tableNames);
        }
    }

    public static class BuilderWithTables {
        public final ActorSystem system;
        public final DataSource dataSource;
        public final TableNames tableNames;

        public BuilderWithTables(ActorSystem system, DataSource dataSource, TableNames tableNames) {
            this.system = system;
            this.dataSource = dataSource;
            this.tableNames = tableNames;
        }

        public BuilderWithTx withTransactionManager(TransactionManager<Connection> transactionManager, ExecutorService executor) {
            return new BuilderWithTx(system, dataSource, tableNames, transactionManager, executor);
        }

        public BuilderWithTx withTransactionManager(ExecutorService executor) {
            return new BuilderWithTx(system, dataSource, tableNames, new JdbcTransactionManager(dataSource, executor), executor);
        }
    }

    public static class BuilderWithTx {
        public final ActorSystem system;
        public final DataSource dataSource;
        public final TableNames tableNames;
        public final TransactionManager<Connection> transactionManager;
        public final ExecutorService executor;

        public BuilderWithTx(ActorSystem system, DataSource dataSource, TableNames tableNames,
                TransactionManager<Connection> transactionManager, ExecutorService executor) {
            this.system = system;
            this.dataSource = dataSource;
            this.tableNames = tableNames;
            this.transactionManager = transactionManager;
            this.executor = executor;
        }

        public <E extends Event> BuilderWithEventFormat<E> withEventFormater(JacksonEventFormat<?, E> eventFormat) {
            return new BuilderWithEventFormat<>(system, dataSource, tableNames, transactionManager, eventFormat, executor);
        }
    }

    public static class BuilderWithEventFormat<E extends Event> {
        public final ActorSystem system;
        public final DataSource dataSource;
        public final TableNames tableNames;
        public final TransactionManager<Connection> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final ExecutorService executor;

        public BuilderWithEventFormat(ActorSystem system, DataSource dataSource, TableNames tableNames,
                TransactionManager<Connection> transactionManager, JacksonEventFormat<?, E> eventFormat, ExecutorService executor) {
            this.system = system;
            this.dataSource = dataSource;
            this.tableNames = tableNames;
            this.transactionManager = transactionManager;
            this.eventFormat = eventFormat;
            this.executor = executor;
        }

        public <Meta> BuilderWithMetaFormat<E, Meta> withMetaFormater(JacksonSimpleFormat<Meta> metaFormat) {
            return new BuilderWithMetaFormat<E, Meta>(system, dataSource, tableNames, transactionManager, eventFormat, metaFormat, executor);
        }

        public BuilderWithMetaFormat<E, Tuple0> withNoMetaFormater() {
            return new BuilderWithMetaFormat<E, Tuple0>(system, dataSource, tableNames, transactionManager, eventFormat, JacksonSimpleFormat.<Tuple0>empty(), executor);
        }
    }

    public static class BuilderWithMetaFormat<E extends Event, Meta> {
        public final ActorSystem system;
        public final DataSource dataSource;
        public final TableNames tableNames;
        public final TransactionManager<Connection> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final JacksonSimpleFormat<Meta> metaFormat;
        public final ExecutorService executor;

        public BuilderWithMetaFormat(ActorSystem system, DataSource dataSource, TableNames tableNames,
                TransactionManager<Connection> transactionManager, JacksonEventFormat<?, E> eventFormat,
                JacksonSimpleFormat<Meta> metaFormat, ExecutorService executor) {
            this.system = system;
            this.dataSource = dataSource;
            this.tableNames = tableNames;
            this.transactionManager = transactionManager;
            this.eventFormat = eventFormat;
            this.metaFormat = metaFormat;
            this.executor = executor;
        }

        public <Context> BuilderWithContextFormat<E, Meta, Context> withContextFormater(JacksonSimpleFormat<Context> contextFormat) {
            return new BuilderWithContextFormat<E, Meta, Context>(system, dataSource, tableNames, transactionManager, eventFormat, metaFormat, contextFormat, executor);
        }

        public BuilderWithContextFormat<E, Meta, Tuple0> withNoContextFormater() {
            return new BuilderWithContextFormat<E, Meta, Tuple0>(system, dataSource, tableNames, transactionManager, eventFormat, metaFormat, JacksonSimpleFormat.<Tuple0>empty(), executor);
        }
    }

    public static class BuilderWithContextFormat<E extends Event, Meta, Context> {
        public final ActorSystem system;
        public final DataSource dataSource;
        public final TableNames tableNames;
        public final TransactionManager<Connection> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final JacksonSimpleFormat<Meta> metaFormat;
        public final JacksonSimpleFormat<Context> contextFormat;
        public final ExecutorService executor;

        public BuilderWithContextFormat(ActorSystem system, DataSource dataSource, TableNames tableNames,
                TransactionManager<Connection> transactionManager, JacksonEventFormat<?, E> eventFormat,
                JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat, ExecutorService executor) {
            this.system = system;
            this.dataSource = dataSource;
            this.tableNames = tableNames;
            this.transactionManager = transactionManager;
            this.eventFormat = eventFormat;
            this.metaFormat = metaFormat;
            this.contextFormat = contextFormat;
            this.executor = executor;
        }

        public BuilderWithKafkaSettings<E, Meta, Context> withKafkaSettings(String topic, ProducerSettings<String, EventEnvelope<E, Meta, Context>> producerSettings, Integer bufferSize) {
            return new BuilderWithKafkaSettings<>(
                    system,
                    dataSource,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    topic,
                    producerSettings,
                    bufferSize, executor, null);
        }

        public BuilderWithKafkaSettings<E, Meta, Context> withKafkaSettings(String topic, ProducerSettings<String, EventEnvelope<E, Meta, Context>> producerSettings) {
            return withKafkaSettings(topic, producerSettings, 1000);
        }
    }

    public static class BuilderWithKafkaSettings<E extends Event, Meta, Context> {
        public final ActorSystem system;
        public final DataSource dataSource;
        public final TableNames tableNames;
        public final TransactionManager<Connection> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final JacksonSimpleFormat<Meta> metaFormat;
        public final JacksonSimpleFormat<Context> contextFormat;
        public final ConcurrentReplayStrategy concurrentReplayStrategy;
        public final KafkaEventPublisher<E, Meta, Context> eventPublisher;
        public final PostgresEventStore<E, Meta, Context> eventStore;
        public final ExecutorService executor;

        BuilderWithKafkaSettings(ActorSystem system, DataSource dataSource, TableNames tableNames, TransactionManager<Connection> transactionManager, JacksonEventFormat<?, E> eventFormat, JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat, ConcurrentReplayStrategy concurrentReplayStrategy, KafkaEventPublisher<E, Meta, Context> eventPublisher, PostgresEventStore<E, Meta, Context> eventStore, ExecutorService executor) {
            this.system = system;
            this.dataSource = dataSource;
            this.tableNames = tableNames;
            this.transactionManager = transactionManager;
            this.eventFormat = eventFormat;
            this.metaFormat = metaFormat;
            this.contextFormat = contextFormat;
            this.concurrentReplayStrategy = concurrentReplayStrategy;
            this.eventPublisher = eventPublisher;
            this.eventStore = eventStore;
            this.executor = executor;
        }

        BuilderWithKafkaSettings(ActorSystem system, DataSource dataSource, TableNames tableNames, TransactionManager<Connection> transactionManager, JacksonEventFormat<?, E> eventFormat, JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat, String topic, ProducerSettings<String, EventEnvelope<E, Meta, Context>> producerSettings, Integer bufferSize, ExecutorService executor, ConcurrentReplayStrategy concurrentReplayStrategy) {
            this.system = system;
            this.dataSource = dataSource;
            this.tableNames = tableNames;
            this.transactionManager = transactionManager;
            this.eventFormat = eventFormat;
            this.metaFormat = metaFormat;
            this.contextFormat = contextFormat;
            this.executor = executor;
            this.concurrentReplayStrategy = Option.of(concurrentReplayStrategy).getOrElse(WAIT);
            this.eventPublisher = new KafkaEventPublisher<>(system, producerSettings, topic, bufferSize);
            this.eventStore = new PostgresEventStore<>(
                    system,
                    eventPublisher,
                    dataSource,
                    executor,
                    tableNames,
                    eventFormat,
                    metaFormat,
                    contextFormat
            );
        }


        public BuilderWithKafkaSettings<E, Meta, Context> withSkipConcurrentReplayStrategy() {
            return new BuilderWithKafkaSettings<>(
                    system,
                    dataSource,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    ConcurrentReplayStrategy.SKIP,
                    eventPublisher,
                    eventStore,
                    executor
            );
        }

        public BuilderWithKafkaSettings<E, Meta, Context> withWaitConcurrentReplayStrategy() {
            return new BuilderWithKafkaSettings<>(
                    system,
                    dataSource,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    ConcurrentReplayStrategy.WAIT,
                    eventPublisher,
                    eventStore,
                    executor
            );
        }

        public BuilderWithKafkaSettings<E, Meta, Context> withNoConcurrentReplayStrategy() {
            return new BuilderWithKafkaSettings<>(
                    system,
                    dataSource,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    ConcurrentReplayStrategy.NO_STRATEGY,
                    eventPublisher,
                    eventStore,
                    executor
            );
        }

        public BuilderWithKafkaSettings<E, Meta, Context> withConcurrentReplayStrategy(ConcurrentReplayStrategy concurrentReplayStrategy) {
            return new BuilderWithKafkaSettings<>(
                    system,
                    dataSource,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    concurrentReplayStrategy,
                    eventPublisher,
                    eventStore,
                    executor
            );
        }

        public <S extends State<S>> BuilderWithEventHandler<S, E, Meta, Context> withEventHandler(EventHandler<S, E> eventHandler) {
            return new BuilderWithEventHandler<>(
                    system,
                    dataSource,
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
        public final DataSource dataSource;
        public final TableNames tableNames;
        public final TransactionManager<Connection> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final JacksonSimpleFormat<Meta> metaFormat;
        public final JacksonSimpleFormat<Context> contextFormat;
        public final KafkaEventPublisher<E, Meta, Context> eventPublisher;
        public final ConcurrentReplayStrategy concurrentReplayStrategy;
        public final PostgresEventStore<E, Meta, Context> eventStore;
        public final EventHandler<S, E> eventHandler;

        public BuilderWithEventHandler(ActorSystem system, DataSource dataSource, TableNames tableNames,
                TransactionManager<Connection> transactionManager, JacksonEventFormat<?, E> eventFormat,
                JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat,
                KafkaEventPublisher<E, Meta, Context> eventPublisher, ConcurrentReplayStrategy concurrentReplayStrategy,
                PostgresEventStore<E, Meta, Context> eventStore, EventHandler<S, E> eventHandler) {
            this.system = system;
            this.dataSource = dataSource;
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

        public BuilderWithAggregateStore<S, E, Meta, Context> withAggregateStore(Function<BuilderWithEventHandler<S, E, Meta, Context>, ? extends AggregateStore<S, String, Connection>> builder) {
            return new BuilderWithAggregateStore<>(
                    system,
                    dataSource,
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

        public BuilderWithAggregateStore<S, E, Meta, Context> withAggregateStore(AggregateStore<S, String, Connection> aggregateStore) {
            return new BuilderWithAggregateStore<>(
                    system,
                    dataSource,
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
                    dataSource,
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
        public final DataSource dataSource;
        public final TableNames tableNames;
        public final TransactionManager<Connection> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final JacksonSimpleFormat<Meta> metaFormat;
        public final JacksonSimpleFormat<Context> contextFormat;
        public final KafkaEventPublisher<E, Meta, Context> eventPublisher;
        public final ConcurrentReplayStrategy concurrentReplayStrategy;
        public final PostgresEventStore<E, Meta, Context> eventStore;
        public final EventHandler<S, E> eventHandler;
        public final AggregateStore<S, String, Connection> aggregateStore;

        public BuilderWithAggregateStore(ActorSystem system, DataSource dataSource, TableNames tableNames,
                TransactionManager<Connection> transactionManager, JacksonEventFormat<?, E> eventFormat,
                JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat,
                KafkaEventPublisher<E, Meta, Context> eventPublisher, ConcurrentReplayStrategy concurrentReplayStrategy,
                PostgresEventStore<E, Meta, Context> eventStore, EventHandler<S, E> eventHandler,
                AggregateStore<S, String, Connection> aggregateStore) {
            this.system = system;
            this.dataSource = dataSource;
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

        public <Error, C extends Command<Meta, Context>, Message> BuilderWithCommandHandler<Error, S, C, E, Message, Meta, Context> withCommandHandler(CommandHandler<Error, S, C, E, Message, Connection> commandHandler) {
            return new BuilderWithCommandHandler<>(
                    system,
                    dataSource,
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

        public <Error, C extends Command<Meta, Context>, Message> BuilderWithCommandHandler<Error, S, C, E, Message, Meta, Context> withCommandHandler(Function<BuilderWithAggregateStore<S, E, Meta, Context>, CommandHandler<Error, S, C, E, Message, Connection>> commandHandler) {
            return new BuilderWithCommandHandler<>(
                    system,
                    dataSource,
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
        public final DataSource dataSource;
        public final TableNames tableNames;
        public final TransactionManager<Connection> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final JacksonSimpleFormat<Meta> metaFormat;
        public final JacksonSimpleFormat<Context> contextFormat;
        public final KafkaEventPublisher<E, Meta, Context> eventPublisher;
        public final ConcurrentReplayStrategy concurrentReplayStrategy;
        public final PostgresEventStore<E, Meta, Context> eventStore;
        public final AggregateStore<S, String, Connection> aggregateStore;
        public final EventHandler<S, E> eventHandler;
        public final CommandHandler<Error, S, C, E, Message, Connection> commandHandler;

        public BuilderWithCommandHandler(ActorSystem system, DataSource dataSource, TableNames tableNames,
                TransactionManager<Connection> transactionManager, JacksonEventFormat<?, E> eventFormat,
                JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat,
                KafkaEventPublisher<E, Meta, Context> eventPublisher, ConcurrentReplayStrategy concurrentReplayStrategy,
                PostgresEventStore<E, Meta, Context> eventStore,
                AggregateStore<S, String, Connection> aggregateStore, EventHandler<S, E> eventHandler,
                CommandHandler<Error, S, C, E, Message, Connection> commandHandler) {
            this.system = system;
            this.dataSource = dataSource;
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

        public BuilderWithProjections<Error, S, C, E, Message, Meta, Context> withProjections(List<Projection<Connection, E, Meta, Context>> projections) {
            return new BuilderWithProjections<>(
                    system,
                    dataSource,
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

        public BuilderWithProjections<Error, S, C, E, Message, Meta, Context> withProjections(Function<BuilderWithCommandHandler<Error, S, C, E, Message, Meta, Context>, List<Projection<Connection, E, Meta, Context>>> projections) {
            return new BuilderWithProjections<>(
                    system,
                    dataSource,
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

        public BuilderWithProjections<Error, S, C, E, Message, Meta, Context> withProjections(Projection<Connection, E, Meta, Context>... projections) {
            return withProjections(List.of(projections));
        }

        public BuilderWithProjections<Error, S, C, E, Message, Meta, Context> withNoProjections() {
            return withProjections(List.empty());
        }
    }


    public static class BuilderWithProjections<Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, Message, Meta, Context> {
        public final ActorSystem system;
        public final DataSource dataSource;
        public final TableNames tableNames;
        public final TransactionManager<Connection> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final JacksonSimpleFormat<Meta> metaFormat;
        public final JacksonSimpleFormat<Context> contextFormat;
        public final KafkaEventPublisher<E, Meta, Context> eventPublisher;
        public final ConcurrentReplayStrategy concurrentReplayStrategy;
        public final PostgresEventStore<E, Meta, Context> eventStore;
        public final AggregateStore<S, String, Connection> aggregateStore;
        public final EventHandler<S, E> eventHandler;
        public final CommandHandler<Error, S, C, E, Message, Connection> commandHandler;
        public final List<Projection<Connection, E, Meta, Context>> projections;

        public BuilderWithProjections(ActorSystem system, DataSource dataSource, TableNames tableNames,
                TransactionManager<Connection> transactionManager, JacksonEventFormat<?, E> eventFormat,
                JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat,
                KafkaEventPublisher<E, Meta, Context> eventPublisher, ConcurrentReplayStrategy concurrentReplayStrategy,
                PostgresEventStore<E, Meta, Context> eventStore,
                AggregateStore<S, String, Connection> aggregateStore, EventHandler<S, E> eventHandler,
                CommandHandler<Error, S, C, E, Message, Connection> commandHandler,
                List<Projection<Connection, E, Meta, Context>> projections) {
            this.system = system;
            this.dataSource = dataSource;
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

        private BuilderWithLockManager withLockManager(LockManager<Connection> lockManager) {
            return new BuilderWithLockManager<>(
                system,
                dataSource,
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
                projections,
                lockManager
            );
        }

        public BuilderWithLockManager withLockManager() {
            return this.withLockManager(new PostgresLockManager(tableNames));
        }

        public BuilderWithLockManager withNoLockManager() {
            return this.withLockManager(new NoOpLockManager<>());
        }

        public PostgresKafkaEventProcessor<Error, S, C, E, Message, Meta, Context> build() {
            return new PostgresKafkaEventProcessor<Error, S, C, E, Message, Meta, Context>(
                    new PostgresKafkaEventProcessor.PostgresKafkaEventProcessorConfig<Error, S, C, E, Message, Meta, Context>(
                            concurrentReplayStrategy,
                            eventStore,
                            transactionManager,
                            aggregateStore,
                            commandHandler,
                            eventHandler,
                            projections,
                            eventPublisher,
                            new NoOpLockManager<>() // For compatibility
                    )
            );
        }
    }

    public static class BuilderWithLockManager<Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, Message, Meta, Context> {
        public final ActorSystem system;
        public final DataSource dataSource;
        public final TableNames tableNames;
        public final TransactionManager<Connection> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final JacksonSimpleFormat<Meta> metaFormat;
        public final JacksonSimpleFormat<Context> contextFormat;
        public final KafkaEventPublisher<E, Meta, Context> eventPublisher;
        public final ConcurrentReplayStrategy concurrentReplayStrategy;
        public final PostgresEventStore<E, Meta, Context> eventStore;
        public final AggregateStore<S, String, Connection> aggregateStore;
        public final EventHandler<S, E> eventHandler;
        public final CommandHandler<Error, S, C, E, Message, Connection> commandHandler;
        public final List<Projection<Connection, E, Meta, Context>> projections;
        public final LockManager<Connection> lockManager;

        public BuilderWithLockManager(ActorSystem system, DataSource dataSource, TableNames tableNames,
            TransactionManager<Connection> transactionManager, JacksonEventFormat<?, E> eventFormat,
            JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat,
            KafkaEventPublisher<E, Meta, Context> eventPublisher, ConcurrentReplayStrategy concurrentReplayStrategy,
            PostgresEventStore<E, Meta, Context> eventStore,
            AggregateStore<S, String, Connection> aggregateStore, EventHandler<S, E> eventHandler,
            CommandHandler<Error, S, C, E, Message, Connection> commandHandler,
            List<Projection<Connection, E, Meta, Context>> projections,
            LockManager<Connection> lockManager) {
            this.system = system;
            this.dataSource = dataSource;
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
            this.lockManager = lockManager;
        }

        public PostgresKafkaEventProcessor<Error, S, C, E, Message, Meta, Context> build() {
            return new PostgresKafkaEventProcessor<Error, S, C, E, Message, Meta, Context>(
                new PostgresKafkaEventProcessor.PostgresKafkaEventProcessorConfig<Error, S, C, E, Message, Meta, Context>(
                    concurrentReplayStrategy,
                    eventStore,
                    transactionManager,
                    aggregateStore,
                    commandHandler,
                    eventHandler,
                    projections,
                    eventPublisher,
                    lockManager
                )
            );
        }
    }

}
