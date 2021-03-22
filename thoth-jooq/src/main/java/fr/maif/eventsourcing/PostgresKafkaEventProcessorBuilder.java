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
import fr.maif.eventsourcing.impl.TableNames;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.control.Option;
import lombok.AllArgsConstructor;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy.NO_STRATEGY;
import static fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy.SKIP;

public class PostgresKafkaEventProcessorBuilder {


    @AllArgsConstructor
    public static class BuilderWithSystem {
        public final ActorSystem system;

        public BuilderWithPool withDataSource(DataSource dataSource) {
            return new BuilderWithPool(system, dataSource);
        }
    }

    @AllArgsConstructor
    public static class BuilderWithPool {
        public final ActorSystem system;
        public final DataSource dataSource;

        public BuilderWithTables withTables(TableNames tableNames) {
            return new BuilderWithTables(system, dataSource, tableNames);
        }
    }

    @AllArgsConstructor
    public static class BuilderWithTables {
        public final ActorSystem system;
        public final DataSource dataSource;
        public final TableNames tableNames;

        public BuilderWithTx withTransactionManager(TransactionManager<Connection> transactionManager, ExecutorService executor) {
            return new BuilderWithTx(system, dataSource, tableNames, transactionManager, executor);
        }

        public BuilderWithTx withTransactionManager(ExecutorService executor) {
            return new BuilderWithTx(system, dataSource, tableNames, new JdbcTransactionManager(dataSource, executor), executor);
        }
    }

    @AllArgsConstructor
    public static class BuilderWithTx {
        public final ActorSystem system;
        public final DataSource dataSource;
        public final TableNames tableNames;
        public final TransactionManager<Connection> transactionManager;
        public final ExecutorService executor;

        public <E extends Event> BuilderWithEventFormat<E> withEventFormater(JacksonEventFormat<?, E> eventFormat) {
            return new BuilderWithEventFormat<>(system, dataSource, tableNames, transactionManager, eventFormat, executor);
        }
    }

    @AllArgsConstructor
    public static class BuilderWithEventFormat<E extends Event> {
        public final ActorSystem system;
        public final DataSource dataSource;
        public final TableNames tableNames;
        public final TransactionManager<Connection> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final ExecutorService executor;

        public <Meta> BuilderWithMetaFormat<E, Meta> withMetaFormater(JacksonSimpleFormat<Meta> metaFormat) {
            return new BuilderWithMetaFormat<E, Meta>(system, dataSource, tableNames, transactionManager, eventFormat, metaFormat, executor);
        }

        public BuilderWithMetaFormat<E, Tuple0> withNoMetaFormater() {
            return new BuilderWithMetaFormat<E, Tuple0>(system, dataSource, tableNames, transactionManager, eventFormat, JacksonSimpleFormat.<Tuple0>empty(), executor);
        }
    }

    @AllArgsConstructor
    public static class BuilderWithMetaFormat<E extends Event, Meta> {
        public final ActorSystem system;
        public final DataSource dataSource;
        public final TableNames tableNames;
        public final TransactionManager<Connection> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final JacksonSimpleFormat<Meta> metaFormat;
        public final ExecutorService executor;

        public <Context> BuilderWithContextFormat<E, Meta, Context> withContextFormater(JacksonSimpleFormat<Context> contextFormat) {
            return new BuilderWithContextFormat<E, Meta, Context>(system, dataSource, tableNames, transactionManager, eventFormat, metaFormat, contextFormat, executor);
        }

        public BuilderWithContextFormat<E, Meta, Tuple0> withNoContextFormater() {
            return new BuilderWithContextFormat<E, Meta, Tuple0>(system, dataSource, tableNames, transactionManager, eventFormat, metaFormat, JacksonSimpleFormat.<Tuple0>empty(), executor);
        }
    }

    @AllArgsConstructor
    public static class BuilderWithContextFormat<E extends Event, Meta, Context> {
        public final ActorSystem system;
        public final DataSource dataSource;
        public final TableNames tableNames;
        public final TransactionManager<Connection> transactionManager;
        public final JacksonEventFormat<?, E> eventFormat;
        public final JacksonSimpleFormat<Meta> metaFormat;
        public final JacksonSimpleFormat<Context> contextFormat;
        public final ExecutorService executor;

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
            this.concurrentReplayStrategy = Option.of(concurrentReplayStrategy).getOrElse(NO_STRATEGY);
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

    @AllArgsConstructor
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


    @AllArgsConstructor
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


    @AllArgsConstructor
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


    @AllArgsConstructor
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
                            eventPublisher
                    )
            );
        }

    }

}
